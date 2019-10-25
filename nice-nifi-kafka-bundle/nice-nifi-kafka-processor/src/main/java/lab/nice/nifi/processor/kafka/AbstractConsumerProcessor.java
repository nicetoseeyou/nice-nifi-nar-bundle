package lab.nice.nifi.processor.kafka;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors;
import lab.nice.nifi.processor.kafka.util.KafkaProcessorUtility;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import static lab.nice.nifi.processor.kafka.common.AllowValues.TOPIC_NAME;
import static lab.nice.nifi.processor.kafka.common.AllowValues.TOPIC_PATTERN;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.BOOTSTRAP_SERVERS;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.SECURITY_PROTOCOL;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.AUTO_OFFSET_RESET;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.COMMUNICATION_TIMEOUT;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.GROUP_ID;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.HEADER_ENCODING;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.HEADER_NAME_REGEX;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.HONOR_TRANSACTIONS;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.MAX_BUNDLE_COUNT;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.MAX_BUNDLE_SIZE;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.MAX_POLL_RECORDS;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.MAX_UNCOMMITTED_TIME;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.MESSAGE_DELIMITER;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.TOPICS;
import static lab.nice.nifi.processor.kafka.common.ConsumerPropertyDescriptors.TOPIC_TYPE;
import static lab.nice.nifi.processor.kafka.common.KafkaProcessorConstant.KAFKA_CONSUME_MATCH;
import static lab.nice.nifi.processor.kafka.common.KafkaProcessorConstant.KAFKA_CONSUME_MISMATCH;

public abstract class AbstractConsumerProcessor<K, V> extends AbstractProcessor {
    protected static final List<PropertyDescriptor> DESCRIPTORS;
    protected static final Set<Relationship> RELATIONSHIPS;

    private volatile AbstractConsumerPool<K, V> consumerPool = null;
    private final Set<AbstractConsumerLease<K, V>> activeLeases = Collections.synchronizedSet(new HashSet<>());

    static {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.addAll(CommonPropertyDescriptors.getCommonPropertyDescriptors());
        descriptors.add(TOPICS);
        descriptors.add(TOPIC_TYPE);
        descriptors.add(HONOR_TRANSACTIONS);
        descriptors.add(GROUP_ID);//
        descriptors.add(AUTO_OFFSET_RESET);
        descriptors.add(COMMUNICATION_TIMEOUT);
        descriptors.add(MAX_POLL_RECORDS);//
        descriptors.add(MAX_UNCOMMITTED_TIME);
        descriptors.add(HEADER_ENCODING);
        descriptors.add(HEADER_NAME_REGEX);
        descriptors.add(MESSAGE_DELIMITER);
        descriptors.add(MAX_BUNDLE_COUNT);
        descriptors.add(MAX_BUNDLE_SIZE);

        DESCRIPTORS = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(KAFKA_CONSUME_MATCH);
        relationships.add(KAFKA_CONSUME_MISMATCH);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    protected abstract void addSerdes(final Map<String, Object> kafkaProperties);

    protected abstract AbstractConsumerPool<K, V> poolOf(final int maxConcurrentLeases, final ComponentLog logger,
                                                         final List<String> topics, final Pattern topicPattern,
                                                         final Map<String, Object> kafkaProperties, final boolean honorTransactions,
                                                         final byte[] delimiterBytes, final long maxWaitMilliseconds,
                                                         final long maxBundleSize, final int maxBundleCount,
                                                         final String securityProtocol, final String bootstrapServers,
                                                         final Charset headerCharacterSet, final Pattern headerNamePattern);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName)
                .addValidator(new KafkaProcessorUtility.KafkaConfigValidator(ConsumerConfig.class))
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return KafkaProcessorUtility.validateCommonProperties(validationContext);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AbstractConsumerPool<K, V> pool = getConsumerPool(context);
        if (pool == null) {
            context.yield();
            return;
        }

        try (final AbstractConsumerLease<K, V> lease = pool.obtainConsumer(session, context)) {
            if (lease == null) {
                context.yield();
                return;
            }

            activeLeases.add(lease);
            try {
                while (this.isScheduled() && lease.continuePolling()) {
                    lease.poll();
                }
                if (this.isScheduled() && !lease.commit()) {
                    context.yield();
                }
            } catch (final WakeupException we) {
                getLogger().warn("Was interrupted while trying to communicate with Kafka with lease {}. "
                        + "Will roll back session and discard any partially received data.", new Object[]{lease});
            } catch (final KafkaException kex) {
                getLogger().error("Exception while interacting with Kafka so will close the lease {} due to {}",
                        new Object[]{lease, kex}, kex);
            } catch (final Throwable t) {
                getLogger().error("Exception while processing data from kafka so will close the lease {} due to {}",
                        new Object[]{lease, t}, t);
            } finally {
                activeLeases.remove(lease);
            }
        }
    }

    @OnUnscheduled
    public void interruptActiveThreads() {
        /*
         *There are known issues with the Kafka client library that result in the client code hanging
         *indefinitely when unable to communicate with the broker. In order to address this, we will wait
         *up to 30 seconds for the Threads to finish and then will call Consumer.wakeup() to trigger the
         *thread to wakeup when it is blocked, waiting on a response.
         */
        final long nanosToWait = TimeUnit.SECONDS.toNanos(5L);
        final long start = System.nanoTime();
        while (System.nanoTime() - start < nanosToWait && !activeLeases.isEmpty()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (!activeLeases.isEmpty()) {
            int count = 0;
            for (final AbstractConsumerLease<K, V> lease : activeLeases) {
                getLogger().info("Consumer {} has not finished after waiting 30 seconds; will attempt to wake-up the lease", new Object[]{lease});
                lease.wakeup();
                count++;
            }

            getLogger().info("Woke up {} consumers", new Object[]{count});
        }

        activeLeases.clear();
    }

    @OnStopped
    public void close() {
        final AbstractConsumerPool<K, V> pool = consumerPool;
        consumerPool = null;
        if (pool != null) {
            pool.close();
        }
    }

    private synchronized AbstractConsumerPool<K, V> getConsumerPool(final ProcessContext context) {
        AbstractConsumerPool<K, V> pool = consumerPool;
        if (pool != null) {
            return pool;
        }

        return consumerPool = createConsumerPool(context, getLogger());
    }

    private AbstractConsumerPool<K, V> createConsumerPool(final ProcessContext context, ComponentLog logger) {
        final int maxConcurrentLeases = context.getMaxConcurrentTasks();

        final Map<String, Object> props = new HashMap<>();
        KafkaProcessorUtility.buildCommonKafkaProperties(context, ConsumerConfig.class, props);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        addSerdes(props);

        final String topicListing = context.getProperty(TOPICS).evaluateAttributeExpressions().getValue();
        final String topicType = context.getProperty(TOPIC_TYPE).evaluateAttributeExpressions().getValue();
        final List<String> topics = new ArrayList<>();

        final boolean honorTransactions = context.getProperty(HONOR_TRANSACTIONS).asBoolean();

        final int communicationTimeoutMillis = context.getProperty(COMMUNICATION_TIMEOUT)
                .asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, communicationTimeoutMillis);

        final long maxUncommittedTime = context.getProperty(MAX_UNCOMMITTED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        final String charsetName = context.getProperty(HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);

        final String headerNameRegex = context.getProperty(HEADER_NAME_REGEX).getValue();
        final Pattern headerNamePattern = headerNameRegex == null ? null : Pattern.compile(headerNameRegex);

        final byte[] delimiter = context.getProperty(MESSAGE_DELIMITER).isSet()
                ? context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)
                : null;

        final int maxBundleCount = context.getProperty(MAX_BUNDLE_COUNT).evaluateAttributeExpressions().asInteger();
        final long maxBundleSize = context.getProperty(MAX_BUNDLE_SIZE).evaluateAttributeExpressions().asLong();

        final String securityProtocol = context.getProperty(SECURITY_PROTOCOL).getValue();
        final String bootstrapServers = context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();

        if (topicType.equals(TOPIC_NAME.getValue())) {
            for (final String topic : topicListing.split(",", 100)) {
                final String trimmedName = topic.trim();
                if (!trimmedName.isEmpty()) {
                    topics.add(trimmedName);
                }
            }

            return poolOf(maxConcurrentLeases, logger, topics, null, props, honorTransactions,
                    delimiter, maxUncommittedTime, maxBundleSize, maxBundleCount, securityProtocol,
                    bootstrapServers, charset, headerNamePattern);
        } else if (topicType.equals(TOPIC_PATTERN.getValue())) {
            final Pattern topicPattern = Pattern.compile(topicListing.trim());
            return poolOf(maxConcurrentLeases, logger, null, topicPattern, props, honorTransactions,
                    delimiter, maxUncommittedTime, maxBundleSize, maxBundleCount, securityProtocol,
                    bootstrapServers, charset, headerNamePattern);
        } else {
            logger.error("Subscription type has an unknown value {}", new Object[]{topicType});
            return null;
        }
    }
}
