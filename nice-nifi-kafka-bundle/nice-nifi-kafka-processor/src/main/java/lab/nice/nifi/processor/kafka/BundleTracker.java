package lab.nice.nifi.processor.kafka;

import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.nifi.flowfile.FlowFile;

public class BundleTracker {
    private final String topic;
    private final int partition;
    private final boolean bundled;
    private final boolean matched;
    private final long startOffset;
    private long endOffset;
    private final long startTimestamp;
    private long endTimestamp;
    private long bundleSize = -1L;
    private int bundleCount = 0;
    private volatile FlowFile flowFile;

    public BundleTracker(final String topic, final int partition, final boolean bundled,
                         final boolean matched, final long startOffset, final long startTimestamp) {
        this.topic = topic;
        this.partition = partition;
        this.bundled = bundled;
        this.matched = matched;
        this.startOffset = startOffset;
        this.endOffset = startOffset;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = startTimestamp;
    }

    public BundleTracker(ConsumerRecord<?, ?> record, final boolean bundled, final boolean matched) {
        this(record.topic(), record.partition(), bundled, matched, record.offset(), record.timestamp());
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public boolean isBundled() {
        return bundled;
    }

    public boolean isMatched() {
        return matched;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public long getBundleSize() {
        return bundleSize;
    }

    public int getBundleCount() {
        return bundleCount;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public void setEndOffset(final long endOffset) {
        this.endOffset = endOffset;
    }

    public void setEndTimestamp(final long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public void updateFlowFile(FlowFile flowFile) {
        this.flowFile = flowFile;
    }

    public void increaseCount() {
        this.bundleCount++;
    }

    public void increaseSize(final long recordSize) {
        if (recordSize >= 0) {
            if (bundleSize < 0) {
                bundleSize = recordSize;
            } else {
                bundleSize += recordSize;
            }
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BundleTracker that = (BundleTracker) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(partition, that.partition) &&
                Objects.equals(bundled, that.bundled) &&
                Objects.equals(matched, that.matched) &&
                Objects.equals(startOffset, that.startOffset) &&
                Objects.equals(endOffset, that.endOffset) &&
                Objects.equals(startTimestamp, that.startTimestamp) &&
                Objects.equals(endTimestamp, that.endTimestamp) &&
                Objects.equals(bundleSize, that.bundleSize) &&
                Objects.equals(bundleCount, that.bundleCount) &&
                Objects.equals(flowFile, that.flowFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, bundled, matched, startOffset, endOffset, startTimestamp, endTimestamp, bundleSize, bundleCount, flowFile);
    }

    @Override
    public String toString() {
        return "BundleTracker{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", bundled=" + bundled +
                ", matched=" + matched +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", startTimestamp=" + startTimestamp +
                ", endTimestamp=" + endTimestamp +
                ", bundleSize=" + bundleSize +
                ", bundleCount=" + bundleCount +
                ", flowFile=" + flowFile +
                '}';
    }
}
