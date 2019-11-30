### Description
Consumes JSON messages from Apache Kafka specifically built against the Kafka 2.0 Consumer API.

### Tags
Kafka, Get, Ingest, Ingress, Topic, PubSub, Consume, JSON, 2.0

### Properties
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values, and whether a property supports the [NiFi Expression Language](http://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html).  
    
| Name | Default Value | Allowable Values | Description |
| :--- | :--- | :--- | :--- |
| **Kafka Brokers** | localhost:9092 | | A comma-separated list of known Kafka Brokers in the format <host>:<port> <br>**Supports Expression Language: true (will be evaluated using variable registry only)** |
| **Security Protocol** | PLAINTEXT | PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL | Protocol used to communicate with brokers. Corresponds to Kafka's 'security.protocol' property. |
| Kerberos Service Name | | | The service name that matches the primary name of the Kafka server configured in the broker JAAS file.This can be defined either in Kafka's JAAS config or in Kafka's config. Corresponds to Kafka's 'security.protocol' property.It is ignored unless one of the SASL options of the <Security Protocol> are selected. <br> **Supports Expression Language: true (will be evaluated using variable registry only)** |
| Kerberos Credentials Service | | **Controller Service API:** <br> KerberosCredentialsService <br> **Implementation:**<br> KeytabCredentialsService | Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos |
| Kerberos Principal | | | The Kerberos principal that will be used to connect to brokers. If not set, it is expected to set a JAAS configuration file in the JVM properties defined in the bootstrap.conf file. This principal will be set into 'sasl.jaas.config' Kafka's property. <br> **Supports Expression Language: true (will be evaluated using variable registry only)**|
| Kerberos Keytab | | | The Kerberos keytab that will be used to connect to brokers. If not set, it is expected to set a JAAS configuration file in the JVM properties defined in the bootstrap.conf file. This principal will be set into 'sasl.jaas.config' Kafka's property. <br> **Supports Expression Language: true (will be evaluated using variable registry only)** |
| SSL Context Service | | **Controller Service API:**<br> SSLContextService <br> **Implementations:** <br>StandardRestrictedSSLContextService <br>StandardSSLContextService | Specifies the SSL Context Service to use for communicating with Kafka. |
| **Topic Name(s)** | | | The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated. |
| **Topic Name Format** | names | names/pattern | Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression |
| **Honor Transactions** | true | true/false | Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an "isolation level" of read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If this value is true, NiFi will not receive any messages for which the producer's transaction was canceled, but this can result in some latency since the consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available. |
| **Group ID** | | | A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property. <br> **Supports Expression Language: true (will be evaluated using variable registry only)** |
| **Offset Reset** | latest | earliest/latest/none | Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property. |
| Message Delimiter | | | Since KafkaConsumer receives messages in batches, you have an option to output FlowFiles which contains all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received will result in a single FlowFile which time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS |
| Message Header Encoding | UTF-8 | | Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. This property indicates the Character Encoding to use for deserializing the headers. |
| Headers to Add as Attributes (Regex) | | | A Regular Expression that is matched against all message headers. Any message header whose name matches the regex will be added to the FlowFile as an Attribute. If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like ".*" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling the messages together efficiently. |
| **Communications Timeout** | 60 secs | | Specifies the timeout that the consumer should use when communicating with the Kafka Broker |
| Max Uncommitted Time | 1 secs | | Specifies the maximum amount of time allowed to pass before offsets must be committed. This value impacts how often offsets will be committed. Committing offsets less often increases throughput but also increases the window of potential data duplication in the event of a rebalance or JVM restart between commits. This value is also related to maximum poll records and the use of a message demarcator. When using a message demarcator we can have far more uncommitted messages than when we're not as there is much less for us to keep track of in memory.|
| Max Poll Records | 10000 | | Specifies the maximum number of records Kafka should return in a single poll. |
| Max Bundle Count | 100 | | Specifies the maximum number of the records in a single FlowFile content if the message delimiter is set and writing records in single FlowFile|
| Max Bundle Size | 1048576 | | Specifies the maximum size in bytes of the bundled FlowFile content if the message delimiter is set and writing records in single FlowFile |

### Dynamic Properties
Dynamic Properties allow the user to specify both the name and value of a property.

| Name | Value | Description |
|:---|:---|:---|
| The name of a Kafka configuration property | The value of a given Kafka configuration property | These properties will be added on the Kafka configuration after loading any provided configuration properties. In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged. For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. <br> **Supports Expression Language: true (will be evaluated using variable registry only)** |

### Relationships
| Name | Description |
|:---|:---|
| match | FlowFiles received from Kafka and matched |
| mismatch | FlowFiles received from Kafka but mismatched |

### Reads Attributes
None specified.

### Writes Attributes
| Name | Description | Remark |
|:---|:---|:---|
| kafka.topic | The topic name of the record(s) | |
| kafka.partition | The topic partition number of the record(s) | |
| kafka.offset | The offset of the record | |
| kafka.offset.begin | The offset of the first record in the bundled FlowFile | Only for bundle mode |
| kafka.offset.end | The offset of the last record in the bundled FlowFile | Only for bundle mode |
| kafka.timestamp | The timestamp of the record | |
| kafka.timestamp.begin | The timestamp of the first record in the bundle FlowFile | Only for bundle mode |
| kafka.timestamp.offset | The timestamp of the last record in the bundled FlowFile | Only for bundle mode |
| kafka.record.count | The count of record in the FlowFile | |
| kafka.record.size | The size in bytes of record in the FlowFile | |
| kafka.record.bundled | Indicate whether the FlowFile is bundled | |
| kafka.record.matched | Indicate whether the record(s) in the FlowFile matches given rule | |

### State management
This component does not store state.

### Restricted
This component is not restricted.

### Input requirement
This component does not allow an incoming relationship.

### System Resource Considerations
None specified.