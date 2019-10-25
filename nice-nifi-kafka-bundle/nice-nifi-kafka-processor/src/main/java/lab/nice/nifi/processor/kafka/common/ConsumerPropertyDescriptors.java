package lab.nice.nifi.processor.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import static lab.nice.nifi.processor.kafka.common.AllowValues.OFFSET_EARLIEST;
import static lab.nice.nifi.processor.kafka.common.AllowValues.OFFSET_LATEST;
import static lab.nice.nifi.processor.kafka.common.AllowValues.OFFSET_NONE;
import static lab.nice.nifi.processor.kafka.common.AllowValues.TOPIC_NAME;
import static lab.nice.nifi.processor.kafka.common.AllowValues.TOPIC_PATTERN;


public final class ConsumerPropertyDescriptors {
    private ConsumerPropertyDescriptors() {
    }

    public static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name(s)")
            .description("The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor TOPIC_TYPE = new PropertyDescriptor.Builder()
            .name("topic.type")
            .displayName("Topic Name Format")
            .description("Specifies whether the Topic(s) provided are a comma separated list of names " +
                    "or a single regular expression.")
            .required(true)
            .allowableValues(TOPIC_NAME, TOPIC_PATTERN)
            .defaultValue(TOPIC_NAME.getValue())
            .build();

    public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.GROUP_ID_CONFIG)
            .displayName("Group ID")
            .description("A Group ID is used to identify consumers that are within the same consumer group. " +
                    "Corresponds to Kafka's 'group.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
            .displayName("Offset Reset")
            .description("Allows you to manage the condition when there is no initial offset in Kafka or if " +
                    "the current offset does not exist any more on the server (e.g. because that data has been deleted). " +
                    "Corresponds to Kafka's 'auto.offset.reset' property.")
            .required(true)
            .allowableValues(OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_NONE)
            .defaultValue(OFFSET_LATEST.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
            .name("message.delimiter")
            .displayName("Message Delimiter")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .description("Since KafkaConsumer receives messages in batches, you have an option to output FlowFiles " +
                    "which contains all Kafka messages in a single batch for a given topic and partition and " +
                    "this property allows you to provide a string (interpreted as UTF-8) to use for demarcating apart " +
                    "multiple Kafka messages. This is an optional property and if not provided each Kafka message received " +
                    "will result in a single FlowFile which time it is triggered. To enter special character " +
                    "such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
            .build();

    public static final PropertyDescriptor HONOR_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("honor.transactions")
            .displayName("Honor Transactions")
            .description("Specifies whether or not NiFi should honor transactional guarantees when communicating with " +
                    "Kafka. If false, the Processor will use an \"isolation level\" of read_uncomitted. This means that " +
                    "messages will be received as soon as they are written to Kafka but will be pulled, even if the " +
                    "producer cancels the transactions. If this value is true, NiFi will not receive any messages for " +
                    "which the producer's transaction was canceled, but this can result in some latency since the " +
                    "consumer must wait for the producer to finish its entire transaction instead of pulling as the " +
                    "messages become available.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor HEADER_NAME_REGEX = new PropertyDescriptor.Builder()
            .name("header.name.regex")
            .displayName("Headers to Add as Attributes (Regex)")
            .description("A Regular Expression that is matched against all message headers. Any message header " +
                    "whose name matches the regex will be added to the FlowFile as an Attribute. If not specified, " +
                    "no Header values will be added as FlowFile attributes. If two messages have a different " +
                    "value for the same header and that header is selected by the provided regex, then those two " +
                    "messages must be added to different FlowFiles. As a result, users should be cautious about " +
                    "using a regex like \".*\" if messages are expected to have header values that are unique " +
                    "per message, such as an identifier or timestamp, because it will prevent NiFi from " +
                    "bundling the messages together efficiently.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("header.encoding")
            .displayName("Message Header Encoding")
            .description("Any message header that is found on a Kafka message will be added to " +
                    "the outbound FlowFile as an attribute. This property indicates the " +
                    "Character Encoding to use to deserialize the headers.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(false)
            .build();

    public static final PropertyDescriptor COMMUNICATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("communication.timeout")
            .displayName("Communications Timeout")
            .description("Specifies the timeout that the consumer should use when communicating with the Kafka Broker")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_UNCOMMITTED_TIME = new PropertyDescriptor.Builder()
            .name("max.uncommitted.time")
            .displayName("Max Uncommitted Time")
            .description("Specifies the maximum amount of time allowed to pass before offsets must be committed. " +
                    "This value impacts how often offsets will be committed. Committing offsets less often increases " +
                    "throughput but also increases the window of potential data duplication in the event of a rebalance " +
                    "or JVM restart between commits. This value is also related to maximum poll records and the use of " +
                    "a message demarcation. When using a message demarcator we can have far more uncommitted messages " +
                    "than when we're not as there is much less for us to keep track of in memory.")
            .required(false)
            .defaultValue("1 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder()
            .name("max.poll.records")
            .displayName("Max Poll Records")
            .description("Specifies the maximum number of records Kafka should return in a single poll.")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BUNDLE_COUNT = new PropertyDescriptor.Builder()
            .name("max.bundle.count")
            .displayName("Max Bundle Count")
            .description("Specifies the maximum number of the records in a single FlowFile content if the message " +
                    "delimiter is set and writing records in single FlowFile")
            .required(false)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_BUNDLE_SIZE = new PropertyDescriptor.Builder()
            .name("max.bundle.size")
            .displayName("Max Bundle Size")
            .description("Specifies the maximum size in bytes of the bundled FlowFile content if the message delimiter " +
                    "is set and writing records in single FlowFile")
            .required(false)
            .defaultValue("1048576")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
}
