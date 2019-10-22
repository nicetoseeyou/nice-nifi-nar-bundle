package lab.nice.nifi.processor.kafka.common;

public final class KafkaWriteAttribute {
    private KafkaWriteAttribute() {
    }

    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_PARTITION = "kafka.partition";
    public static final String KAFKA_OFFSET = "kafka.offset";
    public static final String KAFKA_BEGIN_OFFSET = "kafka.offset.begin";
    public static final String KAFKA_END_OFFSET = "kafka.offset.end";
    public static final String KAFKA_TIMESTAMP = "kafka.timestamp";
    public static final String KAFKA_BEGIN_TIMESTAMP = "kafka.timestamp.begin";
    public static final String KAFKA_END_TIMESTAMP = "kafka.timestamp.offset";
    public static final String KAFKA_RECORD_COUNT = "kafka.record.count";
    public static final String KAFKA_RECORD_SIZE = "kafka.record.size";
    public static final String KAFKA_RECORD_BUNDLED = "kafka.record.bundled";
    public static final String KAFKA_RECORD_MATCHED = "kafka.record.matched";
}
