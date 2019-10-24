package lab.nice.nifi.processor.kafka;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lab.nice.nifi.processor.kafka.common.KafkaProcessorConstant;
import lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute;
import lab.nice.nifi.processor.kafka.util.KafkaProcessorUtility;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Abstract Consumer lease with default behaviours
 *
 * @param <K> the type of Kafka ConsumerRecord's key
 * @param <V> the type of Kafka ConsumerRecord's value
 */
public abstract class AbstractConsumerLease<K, V> implements Closeable, ConsumerRebalanceListener {

    private final Consumer<K, V> consumer;
    private final ComponentLog logger;
    private final byte[] delimiterBytes;
    private final long maxWaitMilliseconds;
    private final long maxBundleSize;
    private final int maxBundleCount;
    private final String securityProtocol;
    private final String bootstrapServers;
    private final Charset headerCharacterSet;
    private final Pattern headerNamePattern;

    //used for tracking demarcated flowfiles to their TopicPartition so we can append
    //to them on subsequent poll calls
    private final Map<BundleInfo, List<BundleTracker>> bundleMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsetMap = new HashMap<>();
    private boolean poisoned = false;
    private long leaseStartNanos = -1;
    private boolean lastPollEmpty = false;
    private int recordCount = 0;
    private long recordSize = -1L;

    private ConsumerLeaseListener leaseListener;

    private volatile ProcessSession session;
    private volatile ProcessContext context;
    private volatile boolean closedConsumer;

    /**
     * default constructor to create consumer lease.
     * Writing records separately, construct with null delimiter. Otherwise, the delimiter,
     * the maximum bundle count and the maximum bundle size in byte are required to specified.
     * Adding Kafka ConsumerRecord's headers as FlowFile attributes, both the header name pattern
     * and the header charset are required to specified.
     *
     * @param consumer            the associated Kafka consumer
     * @param logger              the logger to report any errors/warnings
     * @param delimiterBytes      bytes to use as delimiter between messages; null or empty means no delimiter
     * @param maxWaitMilliseconds maximum time to wait for a given lease to acquire data before committing
     * @param maxBundleSize       maximum size of a bundle FlowFile content
     * @param maxBundleCount      maximum record count of a bundle FlowFile content
     * @param securityProtocol    the security protocol used
     * @param bootstrapServers    the bootstrap servers
     * @param headerCharacterSet  indicates the Character Encoding to use to deserialize the headers
     * @param headerNamePattern   a Regular Expression that is matched against all message headers
     */
    public AbstractConsumerLease(final Consumer<K, V> consumer, final ComponentLog logger,
                                 final byte[] delimiterBytes, final long maxWaitMilliseconds,
                                 final long maxBundleSize, final int maxBundleCount,
                                 final String securityProtocol, final String bootstrapServers,
                                 final Charset headerCharacterSet, final Pattern headerNamePattern) {
        this.consumer = consumer;
        this.logger = logger;
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
     * Executes a poll on the underlying Kafka Consumer and creates any new
     * flowfiles necessary or appends to existing ones if in demarcation mode.
     */
    public void poll() {
        /*
         * Implementation note:
         * Even if ConsumeKafka is not scheduled to poll due to downstream connection back-pressure is engaged,
         * for longer than session.timeout.ms (defaults to 10 sec), Kafka consumer sends heartbeat from background thread.
         * If this situation lasts longer than max.poll.interval.ms (defaults to 5 min), Kafka consumer sends
         * Leave Group request to Group Coordinator. When ConsumeKafka processor is scheduled again, Kafka client checks
         * if this client instance is still a part of consumer group. If not, it rejoins before polling messages.
         * This behavior has been fixed via Kafka KIP-62 and available from Kafka client 0.10.1.0.
         */
        try {
            final ConsumerRecords<K, V> records = consumer.poll(
                    Duration.ofMillis(KafkaProcessorConstant.DEFAULT_KAFKA_CONSUME_TIMEOUT));
            lastPollEmpty = records.isEmpty();
            processRecords(records);
        } catch (final ProcessException e) {
            throw e;
        } catch (final Throwable t) {
            this.poison();
            throw t;
        }
    }

    /**
     * Notifies Kafka to commit the offsets for the specified topic/partition
     * pairs to the specified offsets w/the given metadata. This can offer
     * higher performance than the other commitOffsets call as it allows the
     * kafka client to collect more data from Kafka before committing the
     * offsets.
     * <p>
     * if false then we didn't do anything and should probably yield if true
     * then we committed new data
     */
    public boolean commit() {
        if (uncommittedOffsetMap.isEmpty()) {
            resetInternalState();
            return false;
        } else {
            try {
                /*
                 * Committing the NiFi session then the offsets means we have an at
                 * least once guarantee here. If we reversed the order we'd have at
                 * most once.
                 */
                for (List<BundleTracker> trackers : bundleMap.values()) {
                    trackers.forEach(tracker -> transfer(session, tracker));
                }
                session.commit();
                final Map<TopicPartition, OffsetAndMetadata> offsets = uncommittedOffsetMap;
                consumer.commitSync(offsets);
                resetInternalState();
                return true;
            } catch (final KafkaException e) {
                poison();
                logger.warn("Duplicates are likely as we were able to commit the process " +
                        "session but received an exception from Kafka while committing offsets.", e);
                throw e;
            } catch (Throwable t) {
                poison();
                throw t;
            }
        }
    }

    public void setupProcess(final ProcessSession session, final ProcessContext context) {
        this.session = session;
        this.context = context;
    }

    /**
     * register listener to the lease
     *
     * @param leaseListener the listener to register
     */
    public void registerListener(final ConsumerLeaseListener leaseListener) {
        this.leaseListener = leaseListener;
    }

    /**
     * Process consumer records
     *
     * @param records the records to process
     */
    protected void processRecords(final ConsumerRecords<K, V> records) {
        records.partitions().stream().forEach(topicPartition -> {
            final List<ConsumerRecord<K, V>> messages = records.records(topicPartition);
            if (!messages.isEmpty()) {
                final long maxOffset;
                if (null != delimiterBytes) {
                    maxOffset = writeDemarcatedData(session, messages, topicPartition);
                } else {
                    maxOffset = writeDirectData(session, messages, topicPartition);
                }
                recordCount += messages.size();
                uncommittedOffsetMap.put(topicPartition, new OffsetAndMetadata(maxOffset + 1L));
            }
        });
    }

    /**
     * Write records in bundle into FlowFile content and separate by the given delimiter
     *
     * @param session        the associated ProcessSession
     * @param records        the records to write
     * @param topicPartition the topic and partition information of the records
     * @return the maximum offset of the records
     */
    protected long writeDemarcatedData(final ProcessSession session, final List<ConsumerRecord<K, V>> records,
                                       final TopicPartition topicPartition) {
        long maxOffset = -1;
        final Map<BundleInfo, List<ConsumerRecord<K, V>>> map = records.stream()
                .collect(Collectors.groupingBy(record -> new BundleInfo(topicPartition, null, evaluateHeaderAttributes(record))));
        for (Map.Entry<BundleInfo, List<ConsumerRecord<K, V>>> entry : map.entrySet()) {
            final BundleInfo bundleInfo = entry.getKey();
            final List<ConsumerRecord<K, V>> recordList = entry.getValue();
            final List<BundleTracker> trackers = bundleMap.getOrDefault(bundleInfo, new ArrayList<>());
            boolean firstRecord = true;
            FlowFile flowFile;
            BundleTracker tracker = null;
            for (final ConsumerRecord<K, V> record : recordList) {
                final boolean matched = recordPredicate(record);
                final byte[] recordInBytes = recordInBytes(record);
                if (firstRecord) {
                    tracker = new BundleTracker(record, true, matched);
                    flowFile = createFlowFile(session, bundleInfo.getAttributes());
                    tracker.updateFlowFile(flowFile);
                    trackers.add(tracker);
                    firstRecord = false;
                } else {
                    if (freshStart(tracker, matched, recordInBytes)) {
                        tracker = new BundleTracker(record, true, matched);
                        flowFile = createFlowFile(session, bundleInfo.getAttributes());
                        tracker.updateFlowFile(flowFile);
                        trackers.add(tracker);
                    }
                }
                flowFile = tracker.getFlowFile();
                if (tracker.getBundleCount() > 0) {
                    flowFile = session.append(flowFile, out -> out.write(delimiterBytes));
                    tracker.increaseSize(delimiterBytes.length);
                    tracker.updateFlowFile(flowFile);
                }
                if (null != recordInBytes) {
                    flowFile = session.append(flowFile, out -> out.write(recordInBytes));
                    tracker.increaseSize(recordInBytes.length);
                    tracker.updateFlowFile(flowFile);
                }
                tracker.increaseCount();
                addWriteAttributes(session, tracker);
                if (maxOffset < record.offset()) {
                    maxOffset = record.offset();
                }
            }
        }
        return maxOffset;
    }

    /**
     * Write record into FlowFile separately
     *
     * @param session        the associated ProcessSession
     * @param records        the records to write
     * @param topicPartition the topic and partition information of the records
     * @return the maximum offset of the records
     */
    protected long writeDirectData(final ProcessSession session, final List<ConsumerRecord<K, V>> records,
                                   final TopicPartition topicPartition) {
        long maxOffset = -1L;
        for (ConsumerRecord<K, V> record : records) {
            final boolean matched = recordPredicate(record);
            final byte[] recordInBytes = recordInBytes(record);
            final BundleTracker tracker = new BundleTracker(record, false, matched);
            FlowFile flowFile = createFlowFile(session, evaluateHeaderAttributes(record));
            if (null != recordInBytes) {
                flowFile = session.write(flowFile, out -> out.write(recordInBytes));
                tracker.increaseSize(recordInBytes.length);
            }
            tracker.updateFlowFile(flowFile);
            tracker.increaseCount();
            addWriteAttributes(session, tracker);
            transfer(session, tracker);
            if (maxOffset < record.offset()) {
                maxOffset = record.offset();
            }
        }
        return maxOffset;
    }

    /**
     * determine whether to use a new FlowFile and tracker
     *
     * @param tracker       the existing tracker
     * @param matched       whether the incoming record is matched
     * @param recordInBytes the incoming record in bytes
     * @return
     */
    protected boolean freshStart(final BundleTracker tracker, final boolean matched, final byte[] recordInBytes) {
        if (tracker.isMatched() ^ matched) {
            return true;
        }
        if (tracker.getBundleCount() >= maxBundleCount) {
            return true;
        }
        if (tracker.getBundleSize() > 0) {
            //record + delimiter + record
            if (null == recordInBytes) {
                return tracker.getBundleSize() + delimiterBytes.length > maxBundleSize;
            } else {
                return tracker.getBundleSize() + delimiterBytes.length + recordInBytes.length > maxBundleSize;
            }
        }
        return false;
    }

    /**
     * create an new FlowFile
     *
     * @param session    the associated ProcessSession
     * @param attributes the attributes to populate into the new FlowFile
     * @return an new FlowFile with the given attributes
     */
    protected FlowFile createFlowFile(final ProcessSession session, final Map<String, String> attributes) {
        FlowFile flowFile = session.create();
        if (null != attributes && !attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }
        return flowFile;
    }

    /**
     * add Kafka attribute to FlowFile
     *
     * @param session the associated ProcessSession
     * @param tracker the FlowFile tracker
     */
    protected void addWriteAttributes(final ProcessSession session, final BundleTracker tracker) {
        final Map<String, String> writeAttributes = new HashMap<>();
        writeAttributes.put(KafkaWriteAttribute.KAFKA_TOPIC, tracker.getTopic());
        writeAttributes.put(KafkaWriteAttribute.KAFKA_PARTITION, String.valueOf(tracker.getPartition()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_MATCHED, String.valueOf(tracker.isMatched()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_BUNDLED, String.valueOf(tracker.isBundled()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_COUNT, String.valueOf(tracker.getBundleCount()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_SIZE, String.valueOf(tracker.getBundleSize()));
        if (tracker.isBundled()) {
            writeAttributes.put(KafkaWriteAttribute.KAFKA_BEGIN_OFFSET, String.valueOf(tracker.getStartOffset()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_END_OFFSET, String.valueOf(tracker.getEndOffset()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_BEGIN_TIMESTAMP, String.valueOf(tracker.getEndTimestamp()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_END_TIMESTAMP, String.valueOf(tracker.getEndTimestamp()));
        } else {
            writeAttributes.put(KafkaWriteAttribute.KAFKA_OFFSET, String.valueOf(tracker.getStartOffset()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_TIMESTAMP, String.valueOf(tracker.getStartTimestamp()));
        }
        writeAttributes.put(CoreAttributes.MIME_TYPE.key(), contentMimeType());
        final FlowFile newFlowFile = session.putAllAttributes(tracker.getFlowFile(), writeAttributes);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        final String transitUri = KafkaProcessorUtility
                .buildKafkaTransitURI(securityProtocol, bootstrapServers, tracker.getTopic());
        session.getProvenanceReporter().receive(newFlowFile, transitUri, executionDurationMillis);
        tracker.updateFlowFile(newFlowFile);
    }

    /**
     * Evaluate header attributes from Kafka {@link Header} if header pattern is set.
     *
     * @param record the incoming kafka consumer record
     * @return matched attributes in map, if no matched header or pattern not set, return empty map
     */
    protected Map<String, String> evaluateHeaderAttributes(final ConsumerRecord<K, V> record) {
        final Map<String, String> headerAttributes = new HashMap<>();
        if (null != headerNamePattern) {
            for (Header header : record.headers()) {
                final String headerKey = header.key();
                if (headerNamePattern.matcher(headerKey).matches()) {
                    headerAttributes.put(headerKey, new String(header.value(), headerCharacterSet));
                }
            }
        }
        return headerAttributes;
    }

    /**
     * transfer FlowFile to next
     *
     * @param session the associated ProcessSession
     * @param tracker the record tracker
     */
    protected void transfer(final ProcessSession session, final BundleTracker tracker) {
        if (tracker.isMatched()) {
            session.transfer(tracker.getFlowFile(), KafkaProcessorConstant.KAFKA_CONSUME_MATCH);
        } else {
            session.transfer(tracker.getFlowFile(), KafkaProcessorConstant.KAFKA_CONSUME_MISMATCH);
        }
    }

    /**
     * return the MIME type of the written content in FlowFile
     *
     * @return the MIME type
     */
    protected abstract String contentMimeType();

    /**
     * Convert the record into FlowFile content to write in bytes
     *
     * @param record the record to convert
     * @return content in bytes
     */
    protected abstract byte[] recordInBytes(final ConsumerRecord<K, V> record);

    /**
     * Indicate whether the record matches a given rule
     *
     * @param record the record to test
     * @return true if the record matches given rule otherwise false
     */
    protected abstract boolean recordPredicate(final ConsumerRecord<K, V> record);

    /**
     * clears out internal state elements excluding session and consumer as
     * those are managed by the pool itself
     */
    private void resetInternalState() {
        bundleMap.clear();
        uncommittedOffsetMap.clear();
        leaseStartNanos = -1;
        lastPollEmpty = false;
        recordCount = 0;
        recordSize = -1L;
    }

    /**
     * Indicates whether we should continue polling for data. If we are not
     * writing data with a demarcator then we're writing individual flow files
     * per kafka message therefore we must be very mindful of memory usage for
     * the flow file objects (not their content) being held in memory. The
     * content of kafka messages will be written to the content repository
     * immediately upon each poll call but we must still be mindful of how much
     * memory can be used in each poll call. We will indicate that we should
     * stop polling our last poll call produced no new results or if we've
     * polling and processing data longer than the specified maximum polling
     * time or if we have reached out specified max flow file limit or if a
     * rebalance has been initiated for one of the partitions we're watching;
     * otherwise true.
     *
     * @return true if should keep polling; false otherwise
     */
    public boolean continuePolling() {
        //stop if the last poll produced new no data
        if (lastPollEmpty) {
            return false;
        }

        //stop if we've gone past our desired max uncommitted wait time
        if (leaseStartNanos < 0) {
            leaseStartNanos = System.nanoTime();
        }
        final long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        if (durationMillis > maxWaitMilliseconds) {
            return false;
        }

        //stop if we've generated enough flowfiles that we need to be concerned about memory usage for the objects
        if (bundleMap.size() > 200) { //a magic number - the number of simultaneous bundles to track
            return false;
        } else {
            return recordCount < 1000;//admittedlly a magic number - good candidate for processor property
        }
    }

    /**
     * Indicates that the underlying session and consumer should be immediately
     * considered invalid. Once closed the session will be rolled back and the
     * pool should destroy the underlying consumer. This is useful if due to
     * external reasons, such as the processor no longer being scheduled, this
     * lease should be terminated immediately.
     */
    private void poison() {
        poisoned = true;
    }

    /**
     * @return true if this lease has been poisoned; false otherwise
     */
    protected boolean isPoisoned() {
        return poisoned;
    }

    /**
     * Trigger the consumer's {@link KafkaConsumer#wakeup() wakeup()} method.
     */
    public void wakeup() {
        consumer.wakeup();
    }

    /**
     * Close the associated Kafka consumer instance
     */
    private void closeConsumer() {
        leaseListener.beforeConsumerClose();
        try {
            consumer.unsubscribe();
        } catch (Exception e) {
            logger.warn("Failed to unsubscribe " + consumer, e);
        }

        try {
            consumer.close();
        } catch (Exception e) {
            logger.warn("Failed while closing " + consumer, e);
        }
    }

    /**
     * close the lease itself, it can be force close
     */
    protected void close(boolean forceClose) {
        if (closedConsumer) {
            return;
        }
        resetInternalState();
        if (session != null) {
            session.rollback();
            setupProcess(null, null);
        }
        if (forceClose || isPoisoned() || !leaseListener.reusable()) {
            closedConsumer = true;
            closeConsumer();
        }
    }

    /**
     * close the lease itself
     */
    @Override
    public void close() {
        resetInternalState();
        close(false);
    }

    /**
     * Kafka will call this method whenever it is about to rebalance the
     * consumers for the given partitions. We'll simply take this to mean that
     * we need to quickly commit what we've got and will return the consumer to
     * the pool. This method will be called during the poll() method call of
     * this class and will be called by the same thread calling poll according
     * to the Kafka API docs. After this method executes the session and kafka
     * offsets are committed and this lease is closed.
     *
     * @param partitions partitions being reassigned
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (logger.isDebugEnabled()) {
            logger.debug("Rebalance Alert: Paritions '{}' revoked for lease '{}' with consumer '{}'",
                    new Object[]{partitions, this, consumer});
        }
        //force a commit here.  Can reuse the session and consumer after this but must commit now
        // to avoid duplicates if kafka reassigns partition
        commit();
    }

    /**
     * This will be called by Kafka when the rebalance has completed. We don't
     * need to do anything with this information other than optionally log it as
     * by this point we've committed what we've got and moved on.
     *
     * @param partitions topic partition set being reassigned
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (logger.isDebugEnabled()) {
            logger.debug("Rebalance Alert: Paritions '{}' assigned for lease '{}' with consumer '{}'",
                    new Object[]{partitions, this, consumer});
        }
    }
}
