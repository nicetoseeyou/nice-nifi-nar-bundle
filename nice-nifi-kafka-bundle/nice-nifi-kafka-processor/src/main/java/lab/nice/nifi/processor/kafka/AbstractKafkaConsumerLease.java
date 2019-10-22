package lab.nice.nifi.processor.kafka;

import lab.nice.nifi.processor.kafka.common.KafkaProcessorConstant;
import lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute;
import lab.nice.nifi.processor.kafka.util.KafkaProcessorUtility;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AbstractKafkaConsumerLease<K, V> implements Closeable, ConsumerRebalanceListener {

    private final Consumer<K, V> consumer;
    private final ComponentLog logger;
    private final long maxWaitMilliseconds;
    private final byte[] delimiterBytes;
    private final String securityProtocol;
    private final String bootstrapServers;

    //used for tracking demarcated flowfiles to their TopicPartition so we can append
    //to them on subsequent poll calls
    private final Map<BundleInfo, BundleTracker> bundleMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsetMap = new HashMap<>();
    private boolean poisoned = false;
    private long leaseStartNanos = -1;
    private boolean lastPollEmpty = false;
    private int recordCount = 0;
    private long recordSize = -1L;

    public AbstractKafkaConsumerLease(final Consumer<K, V> consumer, final ComponentLog logger,
                                      final long maxWaitMilliseconds, final byte[] delimiterBytes,
                                      final String securityProtocol, final String bootstrapServers) {
        this.consumer = consumer;
        this.logger = logger;
        this.maxWaitMilliseconds = maxWaitMilliseconds;
        this.delimiterBytes = delimiterBytes;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
    }

    protected void poll() {
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

    protected void processRecords(final ConsumerRecords<K, V> records) {
        records.partitions().stream().forEach(topicPartition -> {
            final List<ConsumerRecord<K, V>> messages = records.records(topicPartition);
            if (!messages.isEmpty()) {
                final long maxOffset;
                if (null != delimiterBytes) {
                    maxOffset = writeDelimitedData(getProcessSession(), messages, topicPartition);
                } else {
                    maxOffset = writeDirectData(getProcessSession(), messages, topicPartition);
                }
                recordCount += messages.size();
                uncommittedOffsetMap.put(topicPartition, new OffsetAndMetadata(maxOffset + 1L));
            }
        });
    }

    protected long writeDelimitedData(final ProcessSession session, final List<ConsumerRecord<K, V>> records,
                                      final TopicPartition topicPartition) {
        return -1L;
    }

    protected long writeDirectData(final ProcessSession session, final List<ConsumerRecord<K, V>> records,
                                   final TopicPartition topicPartition) {
        long maxOffset = -1L;
        for (ConsumerRecord<K, V> record : records) {
            maxOffset = record.offset();
            FlowFile flowFile = session.create();
            final boolean matched = recordPredicate(record);
            final BundleTracker tracker = new BundleTracker(record, false, matched);
            tracker.increaseCount();
            final byte[] recordInBytes = recordInBytes(record);
            if (null != recordInBytes) {
                flowFile = session.write(flowFile, out -> out.write(recordInBytes));
                tracker.increaseSize(recordInBytes.length);
            }
            tracker.updateFlowFile(flowFile);
            applyWriteAttributes(tracker);
            transfer(session, tracker);
        }
        return maxOffset;
    }

    //TODO make abstract
    protected ProcessSession getProcessSession() {
        return null;
    }

    //TODO make abstract
    protected void transfer(final ProcessSession session, final BundleTracker tracker) {
        if (tracker.isMatched()) {
            session.transfer(tracker.getFlowFile(), KafkaProcessorConstant.KAFKA_CONSUME_MATCH);
        } else {
            session.transfer(tracker.getFlowFile(), KafkaProcessorConstant.KAFKA_CONSUME_MISMATCH);
        }
    }

    protected void applyWriteAttributes(final BundleTracker tracker) {
        final Map<String, String> writeAttributes = new HashMap<>();
        writeAttributes.put(KafkaWriteAttribute.KAFKA_TOPIC, tracker.getTopic());
        writeAttributes.put(KafkaWriteAttribute.KAFKA_PARTITION, String.valueOf(tracker.getPartition()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_MATCHED, String.valueOf(tracker.isMatched()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_BUNDLED, String.valueOf(tracker.isBundled()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_COUNT, String.valueOf(tracker.getRecordCount()));
        writeAttributes.put(KafkaWriteAttribute.KAFKA_RECORD_SIZE, String.valueOf(tracker.getRecordSize()));
        if (tracker.isBundled()) {
            writeAttributes.put(KafkaWriteAttribute.KAFKA_BEGIN_OFFSET, String.valueOf(tracker.getStartOffset()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_END_OFFSET, String.valueOf(tracker.getEndOffset()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_BEGIN_TIMESTAMP, String.valueOf(tracker.getEndTimestamp()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_END_TIMESTAMP, String.valueOf(tracker.getEndTimestamp()));
        } else {
            writeAttributes.put(KafkaWriteAttribute.KAFKA_OFFSET, String.valueOf(tracker.getStartOffset()));
            writeAttributes.put(KafkaWriteAttribute.KAFKA_TIMESTAMP, String.valueOf(tracker.getStartTimestamp()));
        }
        final FlowFile newFlowFile = getProcessSession().putAllAttributes(tracker.getFlowFile(), writeAttributes);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        final String transitUri = KafkaProcessorUtility
                .buildKafkaTransitURI(securityProtocol, bootstrapServers, tracker.getTopic());

        getProcessSession().getProvenanceReporter().receive(newFlowFile, transitUri, executionDurationMillis);
        tracker.updateFlowFile(newFlowFile);
    }

    //TODO make abstract
    protected byte[] recordInBytes(final ConsumerRecord<K, V> record) {
        return null;
    }

    protected boolean recordPredicate(final ConsumerRecord<K, V> record) {
        return true;
    }

    private void poison() {
        this.poisoned = true;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}
