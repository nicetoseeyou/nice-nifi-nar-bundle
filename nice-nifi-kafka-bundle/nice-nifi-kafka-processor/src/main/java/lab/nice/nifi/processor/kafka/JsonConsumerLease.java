package lab.nice.nifi.processor.kafka;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.nifi.logging.ComponentLog;

public class JsonConsumerLease extends AbstractConsumerLease<JsonNode, JsonNode> {
    private static final String MIME_JSON = "application/json";

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
    public JsonConsumerLease(final Consumer<JsonNode, JsonNode> consumer, final ComponentLog logger,
                             final byte[] delimiterBytes, final long maxWaitMilliseconds,
                             final long maxBundleSize, final int maxBundleCount,
                             final String securityProtocol, final String bootstrapServers,
                             final Charset headerCharacterSet, final Pattern headerNamePattern) {
        super(consumer, logger, delimiterBytes, maxWaitMilliseconds, maxBundleSize,
                maxBundleCount, securityProtocol, bootstrapServers, headerCharacterSet, headerNamePattern);
    }

    @Override
    protected String contentMimeType() {
        return MIME_JSON;
    }

    @Override
    protected byte[] recordInBytes(final ConsumerRecord<JsonNode, JsonNode> record) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('{');
        if (null != record.key()) {
            stringBuilder.append("\"key\":");
            stringBuilder.append(record.key());
        }
        if (null != record.key() && null != record.value()) {
            stringBuilder.append(',');
        }
        if (null != record.value()) {
            stringBuilder.append("\"value\":");
            stringBuilder.append(record.value());
        }
        stringBuilder.append('}');
        return stringBuilder.toString().getBytes();
    }

    @Override
    protected boolean recordPredicate(final ConsumerRecord<JsonNode, JsonNode> record) {
        return true;
    }
}
