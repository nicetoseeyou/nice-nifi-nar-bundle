package lab.nice.nifi.processor.kafka;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.serialization.record.RecordSchema;


public class BundleInfo {
    private final TopicPartition topicPartition;
    private final RecordSchema schema;
    private final Map<String, String> attributes;

    public BundleInfo(TopicPartition topicPartition, RecordSchema schema, Map<String, String> attributes) {
        this.topicPartition = topicPartition;
        this.schema = schema;
        this.attributes = attributes;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public RecordSchema getSchema() {
        return schema;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BundleInfo that = (BundleInfo) o;
        return Objects.equals(topicPartition, that.topicPartition) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, schema, attributes);
    }

    @Override
    public String toString() {
        return "BundleInfo{" +
                "topicPartition=" + topicPartition +
                ", schema=" + schema +
                ", attributes=" + attributes +
                '}';
    }
}
