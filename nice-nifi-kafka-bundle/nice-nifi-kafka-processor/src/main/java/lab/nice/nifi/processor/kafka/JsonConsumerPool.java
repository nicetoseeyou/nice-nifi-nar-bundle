package lab.nice.nifi.processor.kafka;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.nifi.logging.ComponentLog;

public class JsonConsumerPool extends AbstractConsumerPool<JsonNode, JsonNode> {


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
    public JsonConsumerPool(final int maxConcurrentLeases, final ComponentLog logger,
                            final List<String> topics, final Pattern topicPattern,
                            final Map<String, Object> kafkaProperties, final boolean honorTransactions,
                            final byte[] delimiterBytes, final long maxWaitMilliseconds,
                            final long maxBundleSize, final int maxBundleCount,
                            final String securityProtocol, final String bootstrapServers,
                            final Charset headerCharacterSet, final Pattern headerNamePattern) {
        super(maxConcurrentLeases, logger, topics, topicPattern, kafkaProperties, honorTransactions,
                delimiterBytes, maxWaitMilliseconds, maxBundleSize, maxBundleCount, securityProtocol,
                bootstrapServers, headerCharacterSet, headerNamePattern);
    }

    @Override
    protected AbstractConsumerLease<JsonNode, JsonNode> createConsumerLease(final Consumer<JsonNode, JsonNode> consumer) {
        return new JsonConsumerLease(consumer, logger, delimiterBytes, maxWaitMilliseconds,
                maxBundleSize, maxBundleCount, securityProtocol, bootstrapServers, headerCharacterSet, headerNamePattern);
    }
}
