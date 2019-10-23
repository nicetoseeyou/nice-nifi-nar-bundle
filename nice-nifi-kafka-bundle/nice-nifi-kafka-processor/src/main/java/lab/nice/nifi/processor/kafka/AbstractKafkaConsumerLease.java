package lab.nice.nifi.processor.kafka;

import lab.nice.nifi.processor.kafka.common.KafkaProcessorConstant;
import lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute;
import lab.nice.nifi.processor.kafka.util.KafkaProcessorUtility;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AbstractKafkaConsumerLease<K, V> implements Closeable, ConsumerRebalanceListener {

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

    private volatile ProcessSession session;
    private volatile ProcessContext processContext;
    private volatile boolean closedConsumer;

    public AbstractKafkaConsumerLease(final Consumer<K, V> consumer, final ComponentLog logger,
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

    public void poll() {
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

    public boolean commit() {
        if (uncommittedOffsetMap.isEmpty()) {
            resetInternalState();
            return false;
        } else {
            try {
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
        this.processContext = context;
    }

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
        writeAttributes.put(CoreAttributes.MIME_TYPE.key(), recordMimeType());
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

    //TODO make abstract
    protected void transfer(final ProcessSession session, final BundleTracker tracker) {
        if (tracker.isMatched()) {
            session.transfer(tracker.getFlowFile(), KafkaProcessorConstant.KAFKA_CONSUME_MATCH);
        } else {
            session.transfer(tracker.getFlowFile(), KafkaProcessorConstant.KAFKA_CONSUME_MISMATCH);
        }
    }

    //TODO make abstract
    protected String recordMimeType() {
        return "";
    }

    //TODO make abstract
    protected byte[] recordInBytes(final ConsumerRecord<K, V> record) {
        return null;
    }

    //TODO make abstract
    protected boolean recordPredicate(final ConsumerRecord<K, V> record) {
        return true;
    }

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

    @Override
    public void close() {
        resetInternalState();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (logger.isDebugEnabled()) {
            logger.debug("Rebalance Alert: Paritions '{}' revoked for lease '{}' with consumer '{}'",
                    new Object[]{partitions, this, consumer});
        }
        //force a commit here.  Can reuse the session and consumer after this but must commit now to avoid duplicates if kafka reassigns partition
        commit();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Paritions '{}' assigned for lease '{}' with consumer '{}'",
                new Object[]{partitions, this, consumer});
    }
}
