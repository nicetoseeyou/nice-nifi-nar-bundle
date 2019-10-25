package lab.nice.nifi.processor.kafka.common;

public final class KafkaWriteAttribute {
    private KafkaWriteAttribute() {
    }

    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_TOPIC_DOCS = "The topic name of the record(s)";

    public static final String KAFKA_PARTITION = "kafka.partition";
    public static final String KAFKA_PARTITION_DOCS = "The topic partition number of the record(s)";

    public static final String KAFKA_OFFSET = "kafka.offset";
    public static final String KAFKA_OFFSET_DOCS = "The offset of the record";

    public static final String KAFKA_BEGIN_OFFSET = "kafka.offset.begin";
    public static final String KAFKA_BEGIN_OFFSET_DOCS = "The offset of the first record in the bundled FlowFile";

    public static final String KAFKA_END_OFFSET = "kafka.offset.end";
    public static final String KAFKA_END_OFFSET_DOCS = "The offset of the last record in the bundled FlowFile";

    public static final String KAFKA_TIMESTAMP = "kafka.timestamp";
    public static final String KAFKA_TIMESTAMP_DOCS = "The timestamp of the record";

    public static final String KAFKA_BEGIN_TIMESTAMP = "kafka.timestamp.begin";
    public static final String KAFKA_BEGIN_TIMESTAMP_DOCS = "The timestamp of the first record in the bundle FlowFile";

    public static final String KAFKA_END_TIMESTAMP = "kafka.timestamp.offset";
    public static final String KAFKA_END_TIMESTAMP_DOCS = "The timestamp of the last record in the bundled FlowFile";

    public static final String KAFKA_RECORD_COUNT = "kafka.record.count";
    public static final String KAFKA_RECORD_COUNT_DOCS = "The count of record in the FlowFile";

    public static final String KAFKA_RECORD_SIZE = "kafka.record.size";
    public static final String KAFKA_RECORD_SIZE_DOCS = "The size in bytes of record in the FlowFile";

    public static final String KAFKA_RECORD_BUNDLED = "kafka.record.bundled";
    public static final String KAFKA_RECORD_BUNDLED_DOCS = "Indicate whether the FlowFile is bundled";

    public static final String KAFKA_RECORD_MATCHED = "kafka.record.matched";
    public static final String KAFKA_RECORD_MATCHED_DOCS = "Indicate whether the record(s) in the FlowFile matches given rule";
}
