package lab.nice.nifi.processor.kafka.common;

import org.apache.nifi.processor.Relationship;

public final class KafkaProcessorConstant {
    private KafkaProcessorConstant() {

    }

    public static final long DEFAULT_KAFKA_CONSUME_TIMEOUT = 10L;

    public static final Relationship KAFKA_CONSUME_MATCH = new Relationship.Builder()
            .name("match")
            .description("FlowFiles received from Kafka and matched")
            .build();

    public static final Relationship KAFKA_CONSUME_MISMATCH = new Relationship.Builder()
            .name("mismatch")
            .description("FlowFiles received from Kafka but mismatched")
            .build();
}
