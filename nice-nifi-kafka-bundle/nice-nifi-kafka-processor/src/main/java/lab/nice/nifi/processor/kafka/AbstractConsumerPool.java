package lab.nice.nifi.processor.kafka;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import lab.nice.nifi.processor.kafka.common.PoolStats;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

public abstract class AbstractConsumerPool<K, V> implements Closeable {

    protected final BlockingQueue<AbstractConsumerLease<K, V>> pooledLeases;

    protected final ComponentLog logger;

    protected final List<String> topics;
    protected final Pattern topicPattern;
    protected final Map<String, Object> kafkaProperties;
    protected final boolean honorTransactions;
    protected final byte[] delimiterBytes;
    protected final long maxWaitMilliseconds;
    protected final long maxBundleSize;
    protected final int maxBundleCount;
    protected final String securityProtocol;
    protected final String bootstrapServers;
    protected final Charset headerCharacterSet;
    protected final Pattern headerNamePattern;

    protected final AtomicLong consumerCreatedCountRef = new AtomicLong();
    protected final AtomicLong consumerClosedCountRef = new AtomicLong();
    protected final AtomicLong leasesObtainedCountRef = new AtomicLong();

    /**
     * Default constructor.
     * Consume with exactly topic names, construct with null topic regex pattern otherwise construct
     * with null topic list.
     * <p>
     * Write bundled records in FlowFile content, the delimiter, maximum bundle count,
     * maximum bundle byte size must be specified.
     *
     * @param maxConcurrentLeases max allowable consumers at once
     * @param logger              the logger to report any errors/warnings
     * @param topics              the topic list
     * @param topicPattern        the topic name Regular Expression pattern
     * @param kafkaProperties     properties to use to initialize kafka consumers
     * @param honorTransactions   specifies whether or not NiFi should honor transactional guarantees
     *                            when communicating with Kafka
     * @param delimiterBytes      bytes to use as delimiter between messages; null or empty means no delimiter
     * @param maxWaitMilliseconds maximum time to wait for a given lease to acquire data before committing
     * @param maxBundleSize       maximum size of a bundle FlowFile content
     * @param maxBundleCount      maximum record count of a bundle FlowFile content
     * @param securityProtocol    the security protocol used
     * @param bootstrapServers    the bootstrap servers
     * @param headerCharacterSet  indicates the Character Encoding to use to deserialize the headers
     * @param headerNamePattern   a Regular Expression that is matched against all message headers
     */
    public AbstractConsumerPool(final int maxConcurrentLeases, final ComponentLog logger,
                                final List<String> topics, final Pattern topicPattern,
                                final Map<String, Object> kafkaProperties, final boolean honorTransactions,
                                final byte[] delimiterBytes, final long maxWaitMilliseconds,
                                final long maxBundleSize, final int maxBundleCount,
                                final String securityProtocol, final String bootstrapServers,
                                final Charset headerCharacterSet, final Pattern headerNamePattern) {
        this.pooledLeases = new ArrayBlockingQueue<>(maxConcurrentLeases);
        this.logger = logger;
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.kafkaProperties = kafkaProperties;
        this.honorTransactions = honorTransactions;
        this.delimiterBytes = delimiterBytes;
        this.maxWaitMilliseconds = maxWaitMilliseconds;
        this.maxBundleSize = maxBundleSize;
        this.maxBundleCount = maxBundleCount;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
    }

    /**
     * Obtains a consumer from the pool if one is available or lazily
     * initializes a new one if deemed necessary.
     *
     * @param session the session for which the consumer lease will be
     *                associated
     * @param context the ProcessContext for which the consumer
     *                lease will be associated
     * @return consumer to use or null if not available or necessary
     */
    public AbstractConsumerLease<K, V> obtainConsumer(final ProcessSession session, final ProcessContext context) {
        AbstractConsumerLease<K, V> lease = pooledLeases.poll();
        if (null == lease) {
            final Consumer<K, V> consumer = createKafkaConsumer();
            consumerCreatedCountRef.incrementAndGet();
            /*
             * For now return a new consumer lease. But we could later elect to
             * have this return null if we determine the broker indicates that
             * the lag time on all topics being monitored is sufficiently low.
             * For now we should encourage conservative use of threads because
             * having too many means we'll have at best useless threads sitting
             * around doing frequent network calls and at worst having consumers
             * sitting idle which could prompt excessive rebalances.
             */
            final AbstractConsumerLease<K, V> freshLease = createConsumerLease(consumer);
            lease.registerListener(new ConsumerLeaseListener() {
                @Override
                public boolean reusable() {
                    return pooledLeases.offer(freshLease);
                }

                @Override
                public void beforeConsumerClose() {
                    consumerClosedCountRef.incrementAndGet();
                }
            });
            lease = freshLease;
            /*
             * This subscription tightly couples the lease to the given
             * consumer. They cannot be separated from then on.
             */
            if (topics != null) {
                consumer.subscribe(topics, lease);
            } else {
                consumer.subscribe(topicPattern, lease);
            }
        }
        lease.setupProcess(session, context);
        leasesObtainedCountRef.incrementAndGet();
        return lease;
    }

    /**
     * Create a exactly type lease instance
     *
     * @param consumer the associated consumer to the lease
     * @return lease instance
     */
    protected abstract AbstractConsumerLease<K, V> createConsumerLease(final Consumer<K, V> consumer);

    /**
     * Exposed as protected method for easier unit testing
     *
     * @return consumer
     * @throws KafkaException if unable to subscribe to the given topics
     */
    protected Consumer<K, V> createKafkaConsumer() {
        final Map<String, Object> properties = new HashMap<>(kafkaProperties);
        if (honorTransactions) {
            properties.put("isolation.level", "read_committed");
        } else {
            properties.put("isolation.level", "read_uncommitted");
        }
        final Consumer<K, V> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    public PoolStats getPoolStats() {
        return new PoolStats(consumerCreatedCountRef.get(), consumerClosedCountRef.get(), leasesObtainedCountRef.get());
    }

    /**
     * Closes all consumers in the pool. Can be safely called repeatedly.
     */
    @Override
    public void close() {
        final List<AbstractConsumerLease<K, V>> leases = new ArrayList<>();
        pooledLeases.drainTo(leases);
        leases.stream().forEach(lease -> lease.close(true));
    }
}
