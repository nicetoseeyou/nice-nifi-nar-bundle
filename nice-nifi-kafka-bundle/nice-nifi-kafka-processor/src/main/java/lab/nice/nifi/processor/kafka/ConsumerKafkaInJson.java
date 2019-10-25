package lab.nice.nifi.processor.kafka;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;

import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_BEGIN_OFFSET;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_BEGIN_OFFSET_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_BEGIN_TIMESTAMP;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_BEGIN_TIMESTAMP_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_END_OFFSET;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_END_OFFSET_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_END_TIMESTAMP;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_END_TIMESTAMP_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_OFFSET;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_OFFSET_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_PARTITION;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_PARTITION_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_BUNDLED;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_BUNDLED_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_COUNT;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_COUNT_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_MATCHED;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_MATCHED_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_SIZE;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_RECORD_SIZE_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_TIMESTAMP;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_TIMESTAMP_DOCS;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_TOPIC;
import static lab.nice.nifi.processor.kafka.common.KafkaWriteAttribute.KAFKA_TOPIC_DOCS;

@CapabilityDescription("Consumes JSON messages from Apache Kafka specifically built against the Kafka 2.0 Consumer API.")
@Tags({"Kafka", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume", "JSON", "2.0"})
@WritesAttributes({
        @WritesAttribute(attribute = KAFKA_TOPIC, description = KAFKA_TOPIC_DOCS),
        @WritesAttribute(attribute = KAFKA_PARTITION, description = KAFKA_PARTITION_DOCS),
        @WritesAttribute(attribute = KAFKA_OFFSET, description = KAFKA_OFFSET_DOCS),
        @WritesAttribute(attribute = KAFKA_BEGIN_OFFSET, description = KAFKA_BEGIN_OFFSET_DOCS),
        @WritesAttribute(attribute = KAFKA_END_OFFSET, description = KAFKA_END_OFFSET_DOCS),
        @WritesAttribute(attribute = KAFKA_TIMESTAMP, description = KAFKA_TIMESTAMP_DOCS),
        @WritesAttribute(attribute = KAFKA_BEGIN_TIMESTAMP, description = KAFKA_BEGIN_TIMESTAMP_DOCS),
        @WritesAttribute(attribute = KAFKA_END_TIMESTAMP, description = KAFKA_END_TIMESTAMP_DOCS),
        @WritesAttribute(attribute = KAFKA_RECORD_COUNT, description = KAFKA_RECORD_COUNT_DOCS),
        @WritesAttribute(attribute = KAFKA_RECORD_SIZE, description = KAFKA_RECORD_SIZE_DOCS),
        @WritesAttribute(attribute = KAFKA_RECORD_BUNDLED, description = KAFKA_RECORD_BUNDLED_DOCS),
        @WritesAttribute(attribute = KAFKA_RECORD_MATCHED, description = KAFKA_RECORD_MATCHED_DOCS)
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@DynamicProperty(name = "The name of a Kafka configuration property",
        value = "The value of a given Kafka configuration property",
        description = "These properties will be added on the Kafka configuration after loading any provided " +
                "configuration properties. In the event a dynamic property represents a property that was already set, " +
                "its value will be ignored and WARN message logged. For the list of available Kafka properties please " +
                "refer to: http://kafka.apache.org/documentation.html#configuration. ")
public class ConsumerKafkaInJson extends AbstractConsumerProcessor<JsonNode, JsonNode> {
    @Override
    protected void addSerdes(Map<String, Object> kafkaProperties) {
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    }

    @Override
    protected AbstractConsumerPool<JsonNode, JsonNode> poolOf(final int maxConcurrentLeases, final ComponentLog logger,
                                                              final List<String> topics, final Pattern topicPattern,
                                                              final Map<String, Object> kafkaProperties, final boolean honorTransactions,
                                                              final byte[] delimiterBytes, final long maxWaitMilliseconds,
                                                              final long maxBundleSize, final int maxBundleCount,
                                                              final String securityProtocol, final String bootstrapServers,
                                                              final Charset headerCharacterSet, final Pattern headerNamePattern) {
        return new JsonConsumerPool(maxConcurrentLeases, logger, topics, topicPattern, kafkaProperties, honorTransactions,
                delimiterBytes, maxWaitMilliseconds, maxBundleSize, maxBundleCount, securityProtocol,
                bootstrapServers, headerCharacterSet, headerNamePattern);
    }
}
