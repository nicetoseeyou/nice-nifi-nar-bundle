package lab.nice.nifi.processor.kafka.common;

import org.apache.nifi.components.AllowableValue;

public final class AllowValues {
    private AllowValues() {
    }

    public static final AllowableValue OFFSET_EARLIEST =
            new AllowableValue("earliest", "earliest", "Automatically reset the offset to the earliest offset");

    public static final AllowableValue OFFSET_LATEST =
            new AllowableValue("latest", "latest", "Automatically reset the offset to the latest offset");

    public static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none",
            "Throw exception to the consumer if no previous offset is found for the consumer's group");

    public static final AllowableValue TOPIC_NAME =
            new AllowableValue("names", "names", "Topic is a full topic name or comma separated list of names");

    public static final AllowableValue TOPIC_PATTERN = new AllowableValue("pattern", "pattern",
            "Topic is a regex using the Java Pattern syntax");

    public static final AllowableValue SEC_PLAINTEXT =
            new AllowableValue("PLAINTEXT", "PLAINTEXT", "PLAINTEXT");

    public static final AllowableValue SEC_SSL = new AllowableValue("SSL", "SSL", "SSL");

    public static final AllowableValue SEC_SASL_PLAINTEXT =
            new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL_PLAINTEXT");

    public static final AllowableValue SEC_SASL_SSL =
            new AllowableValue("SASL_SSL", "SASL_SSL", "SASL_SSL");
}
