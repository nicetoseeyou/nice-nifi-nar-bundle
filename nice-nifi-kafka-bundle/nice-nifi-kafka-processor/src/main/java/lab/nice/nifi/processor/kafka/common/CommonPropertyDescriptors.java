package lab.nice.nifi.processor.kafka.common;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_PLAINTEXT;
import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_SASL_PLAINTEXT;
import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_SASL_SSL;
import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_SSL;


public final class CommonPropertyDescriptors {
    private CommonPropertyDescriptors() {
    }

    public static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
            .displayName("Kafka Brokers")
            .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("localhost:9092")
            .build();

    public static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Protocol used to communicate with brokers. Corresponds to Kafka's 'security.protocol' property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SEC_PLAINTEXT, SEC_SSL, SEC_SASL_PLAINTEXT, SEC_SASL_SSL)
            .defaultValue(SEC_PLAINTEXT.getValue())
            .build();

    public static final PropertyDescriptor JAAS_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.service.name")
            .displayName("Kerberos Service Name")
            .description("The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's " +
                    "JAAS config or in Kafka's config. Corresponds to Kafka's 'security.protocol' property. " +
                    "It is ignored unless one of the SASL options of the <Security Protocol> are selected.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USER_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.principal")
            .displayName("Kerberos Principal")
            .description("The Kerberos principal that will be used to connect to brokers. If not set, it is expected " +
                    "to set a JAAS configuration file in the JVM properties defined in the bootstrap.conf file. " +
                    "This principal will be set into 'sasl.jaas.config' Kafka's property.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USER_KEYTAB = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.keytab")
            .displayName("Kerberos Keytab")
            .description("The Kerberos keytab that will be used to connect to brokers. If not set, it is expected " +
                    "to set a JAAS configuration file in the JVM properties defined in the bootstrap.conf file. " +
                    "This principal will be set into 'sasl.jaas.config' Kafka's property.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service to use for communicating with Kafka.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used " +
                    "for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return Arrays.asList(
                BOOTSTRAP_SERVERS,
                SECURITY_PROTOCOL,
                JAAS_SERVICE_NAME,
                KERBEROS_CREDENTIALS_SERVICE,
                USER_PRINCIPAL,
                USER_KEYTAB,
                SSL_CONTEXT_SERVICE
        );
    }
}
