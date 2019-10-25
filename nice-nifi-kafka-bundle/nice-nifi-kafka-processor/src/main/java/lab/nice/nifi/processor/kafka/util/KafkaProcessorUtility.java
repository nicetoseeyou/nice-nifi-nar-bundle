package lab.nice.nifi.processor.kafka.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.FormatUtils;

import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_SASL_PLAINTEXT;
import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_SASL_SSL;
import static lab.nice.nifi.processor.kafka.common.AllowValues.SEC_SSL;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.JAAS_SERVICE_NAME;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.KERBEROS_CREDENTIALS_SERVICE;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.SECURITY_PROTOCOL;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.SSL_CONTEXT_SERVICE;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.USER_KEYTAB;
import static lab.nice.nifi.processor.kafka.common.CommonPropertyDescriptors.USER_PRINCIPAL;

public final class KafkaProcessorUtility {
    private static final String ALLOW_EXPLICIT_KEYTAB = "NIFI_ALLOW_EXPLICIT_KEYTAB";

    public static String buildKafkaTransitURI(final String securityProtocol, final String brokers, final String topic) {
        StringBuilder builder = new StringBuilder();
        builder.append(securityProtocol);
        builder.append("://");
        builder.append(brokers);
        builder.append("/");
        builder.append(topic);
        return builder.toString();
    }

    public static void buildCommonKafkaProperties(final ProcessContext context, final Class<?> kafkaConfigClass,
                                                  final Map<String, Object> mapToPopulate) {
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            if (propertyDescriptor.equals(SSL_CONTEXT_SERVICE)) {
                // Translate SSLContext Service configuration into Kafka properties
                final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE)
                        .asControllerService(SSLContextService.class);
                if (sslContextService != null && sslContextService.isKeyStoreConfigured()) {
                    mapToPopulate.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslContextService.getKeyStoreFile());
                    mapToPopulate.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslContextService.getKeyStorePassword());
                    final String keyPass = sslContextService.getKeyPassword() ==
                            null ? sslContextService.getKeyStorePassword() : sslContextService.getKeyPassword();

                    mapToPopulate.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPass);
                    mapToPopulate.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslContextService.getKeyStoreType());
                }

                if (sslContextService != null && sslContextService.isTrustStoreConfigured()) {
                    mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslContextService.getTrustStoreFile());
                    mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslContextService.getTrustStorePassword());
                    mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslContextService.getTrustStoreType());
                }
            }

            String propertyName = propertyDescriptor.getName();
            String propertyValue = propertyDescriptor.isExpressionLanguageSupported()
                    ? context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue()
                    : context.getProperty(propertyDescriptor).getValue();

            if (propertyValue != null && !propertyName.equals(USER_PRINCIPAL.getName())
                    && !propertyName.equals(USER_KEYTAB.getName())) {
                /*
                 *If the property name ends in ".ms" then it is a time period. We want to accept either an integer
                 * as number of milliseconds or the standard NiFi time period such as "5 secs"
                 */
                if (propertyName.endsWith(".ms") && !StringUtils.isNumeric(propertyValue.trim())) {
                    // kafka standard time notation
                    propertyValue = String.valueOf(FormatUtils.getTimeDuration(propertyValue.trim(), TimeUnit.MILLISECONDS));
                }

                if (isStaticStringFieldNamePresent(propertyName, kafkaConfigClass, CommonClientConfigs.class,
                        SslConfigs.class, SaslConfigs.class)) {
                    mapToPopulate.put(propertyName, propertyValue);
                }
            }
        }

        String securityProtocol = context.getProperty(SECURITY_PROTOCOL).getValue();
        if (SEC_SASL_PLAINTEXT.getValue().equals(securityProtocol) || SEC_SASL_SSL.getValue().equals(securityProtocol)) {
            setJaasConfig(mapToPopulate, context);
        }
    }

    public static Collection<ValidationResult> validateCommonProperties(final ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();

        String securityProtocol = validationContext.getProperty(SECURITY_PROTOCOL).getValue();

        final String explicitPrincipal = validationContext.getProperty(USER_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = validationContext.getProperty(USER_KEYTAB).evaluateAttributeExpressions().getValue();

        final KerberosCredentialsService credentialsService = validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE)
                .asControllerService(KerberosCredentialsService.class);

        final String resolvedPrincipal;
        final String resolvedKeytab;
        if (credentialsService == null) {
            resolvedPrincipal = explicitPrincipal;
            resolvedKeytab = explicitKeytab;
        } else {
            resolvedPrincipal = credentialsService.getPrincipal();
            resolvedKeytab = credentialsService.getKeytab();
        }

        if (credentialsService != null && (explicitPrincipal != null || explicitKeytab != null)) {
            results.add(new ValidationResult.Builder()
                    .subject("Kerberos Credentials")
                    .valid(false)
                    .explanation("Cannot specify both a Kerberos Credentials Service and a principal/keytab")
                    .build());
        }

        final String allowExplicitKeytabVariable = System.getenv(ALLOW_EXPLICIT_KEYTAB);
        if ("false".equalsIgnoreCase(allowExplicitKeytabVariable) && (explicitPrincipal != null || explicitKeytab != null)) {
            results.add(new ValidationResult.Builder()
                    .subject("Kerberos Credentials")
                    .valid(false)
                    .explanation("The '" + ALLOW_EXPLICIT_KEYTAB + "' system environment variable is configured to forbid " +
                            "explicitly configuring principal/keytab in processors. The Kerberos Credentials Service " +
                            "should be used instead of setting the Kerberos Keytab or Kerberos Principal property.")
                    .build());
        }

        // validates that if one of SASL (Kerberos) option is selected for
        // security protocol, then Kerberos principal is provided as well
        if (SEC_SASL_PLAINTEXT.getValue().equals(securityProtocol) || SEC_SASL_SSL.getValue().equals(securityProtocol)) {
            String jaasServiceName = validationContext.getProperty(JAAS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
            if (jaasServiceName == null || jaasServiceName.trim().length() == 0) {
                results.add(new ValidationResult.Builder()
                        .subject(JAAS_SERVICE_NAME.getDisplayName())
                        .valid(false)
                        .explanation("The <" + JAAS_SERVICE_NAME.getDisplayName() + "> property must be set when <"
                                + SECURITY_PROTOCOL.getDisplayName() + "> is configured as '"
                                + SEC_SASL_PLAINTEXT.getValue() + "' or '" + SEC_SASL_SSL.getValue() + "'.")
                        .build());
            }

            if ((resolvedKeytab == null && resolvedPrincipal != null) || (resolvedKeytab != null && resolvedPrincipal == null)) {
                results.add(new ValidationResult.Builder()
                        .subject(JAAS_SERVICE_NAME.getDisplayName())
                        .valid(false)
                        .explanation("Both <" + USER_KEYTAB.getDisplayName() + "> and <" + USER_PRINCIPAL.getDisplayName() + "> "
                                + "must be set or neither must be set.")
                        .build());
            }
        }

        // If SSL or SASL_SSL then SSLContext Controller Service must be set.
        final boolean sslProtocol = SEC_SSL.getValue().equals(securityProtocol) || SEC_SASL_SSL.getValue().equals(securityProtocol);
        final boolean csSet = validationContext.getProperty(SSL_CONTEXT_SERVICE).isSet();
        if (csSet && !sslProtocol) {
            results.add(new ValidationResult.Builder()
                    .subject(SECURITY_PROTOCOL.getDisplayName())
                    .valid(false)
                    .explanation("If you set the SSL Controller Service you should also choose an SSL based security protocol.")
                    .build());
        }

        if (!csSet && sslProtocol) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("If you set to an SSL based protocol you need to set the SSL Controller Service")
                    .build());
        }

        final PropertyDescriptor autoCommit = new PropertyDescriptor.Builder()
                .name(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                .build();
        final String enableAutoCommit = validationContext.getProperty(autoCommit).getValue();
        if (enableAutoCommit != null && !enableAutoCommit.toLowerCase().equals("false")) {
            final ValidationResult autoCommitResult = new ValidationResult.Builder()
                    .subject(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                    .explanation("Enable auto commit must be false. It is managed by the processor.")
                    .build();
            results.add(autoCommitResult);
        }

        return results;
    }

    public static Collection<ValidationResult> validateByteSerdes(final ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        final PropertyDescriptor keySerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                .build();
        final String keySerializer = validationContext.getProperty(keySerializerDescriptor).getValue();
        if (keySerializer != null && !ByteArraySerializer.class.getName().equals(keySerializer)) {
            final ValidationResult keySerializerResult = new ValidationResult.Builder()
                    .subject(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                    .explanation("Key Serializer must be " + ByteArraySerializer.class.getName() + "' was '"
                            + keySerializer + "'")
                    .build();
            results.add(keySerializerResult);
        }

        final PropertyDescriptor valueSerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                .build();
        final String valueSerializer = validationContext.getProperty(valueSerializerDescriptor).getValue();
        if (valueSerializer != null && !ByteArraySerializer.class.getName().equals(valueSerializer)) {
            final ValidationResult valueSerializerResult = new ValidationResult.Builder()
                    .subject(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                    .explanation("Value Serializer must be " + ByteArraySerializer.class.getName() + "' was '"
                            + valueSerializer + "'")
                    .build();
            results.add(valueSerializerResult);
        }

        final PropertyDescriptor keyDeSerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                .build();
        final String keyDeSerializer = validationContext.getProperty(keyDeSerializerDescriptor).getValue();
        if (keyDeSerializer != null && !ByteArrayDeserializer.class.getName().equals(keyDeSerializer)) {
            final ValidationResult keyDeSerializerResult = new ValidationResult.Builder()
                    .subject(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Key De-Serializer must be '" + ByteArrayDeserializer.class.getName() + "' was '"
                            + keyDeSerializer + "'")
                    .build();
            results.add(keyDeSerializerResult);
        }

        final PropertyDescriptor valueDeSerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                .build();
        final String valueDeSerializer = validationContext.getProperty(valueDeSerializerDescriptor).getValue();
        if (valueDeSerializer != null && !ByteArrayDeserializer.class.getName().equals(valueDeSerializer)) {
            final ValidationResult valueDeSerializerResult = new ValidationResult.Builder()
                    .subject(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Value De-Serializer must be " + ByteArrayDeserializer.class.getName() + "' was '"
                            + valueDeSerializer + "'")
                    .build();
            results.add(valueDeSerializerResult);
        }
        return results;
    }

    public static Collection<ValidationResult> validateJsonSerdes(final ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        final PropertyDescriptor keySerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                .build();
        final String keySerializer = validationContext.getProperty(keySerializerDescriptor).getValue();
        if (keySerializer != null && !JsonSerializer.class.getName().equals(keySerializer)) {
            final ValidationResult keySerializerResult = new ValidationResult.Builder()
                    .subject(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                    .explanation("Key Serializer must be " + JsonSerializer.class.getName() + "' was '"
                            + keySerializer + "'")
                    .build();
            results.add(keySerializerResult);
        }

        final PropertyDescriptor valueSerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                .build();
        final String valueSerializer = validationContext.getProperty(valueSerializerDescriptor).getValue();
        if (valueSerializer != null && !JsonSerializer.class.getName().equals(valueSerializer)) {
            final ValidationResult valueSerializerResult = new ValidationResult.Builder()
                    .subject(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                    .explanation("Value Serializer must be " + JsonSerializer.class.getName() + "' was '"
                            + valueSerializer + "'")
                    .build();
            results.add(valueSerializerResult);
        }

        final PropertyDescriptor keyDeSerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                .build();
        final String keyDeSerializer = validationContext.getProperty(keyDeSerializerDescriptor).getValue();
        if (keyDeSerializer != null && !JsonDeserializer.class.getName().equals(keyDeSerializer)) {
            final ValidationResult keyDeSerializerResult = new ValidationResult.Builder()
                    .subject(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Key De-Serializer must be '" + JsonDeserializer.class.getName() + "' was '"
                            + keyDeSerializer + "'")
                    .build();
            results.add(keyDeSerializerResult);
        }

        final PropertyDescriptor valueDeSerializerDescriptor = new PropertyDescriptor.Builder()
                .name(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                .build();
        final String valueDeSerializer = validationContext.getProperty(valueDeSerializerDescriptor).getValue();
        if (valueDeSerializer != null && !JsonDeserializer.class.getName().equals(valueDeSerializer)) {
            final ValidationResult valueDeSerializerResult = new ValidationResult.Builder()
                    .subject(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Value De-Serializer must be " + JsonDeserializer.class.getName() + "' was '"
                            + valueDeSerializer + "'")
                    .build();
            results.add(valueDeSerializerResult);
        }
        return results;
    }

    /**
     * Method used to configure the 'sasl.jaas.config' property based on KAFKA-4259<br />
     * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients<br />
     * <br />
     * It expects something with the following format: <br />
     * <br />
     * &lt;LoginModuleClass&gt; &lt;ControlFlag&gt; *(&lt;OptionName&gt;=&lt;OptionValue&gt;); <br />
     * ControlFlag = required / requisite / sufficient / optional
     *
     * @param mapToPopulate Map of configuration properties
     * @param context       Context
     */
    private static void setJaasConfig(Map<String, Object> mapToPopulate, ProcessContext context) {
        String keytab = context.getProperty(USER_KEYTAB).evaluateAttributeExpressions().getValue();
        String principal = context.getProperty(USER_PRINCIPAL).evaluateAttributeExpressions().getValue();

        /*
         *If the Kerberos Credentials Service is specified, we need to use its configuration, not the explicit
         * properties for principal/keytab. The customValidate method ensures that only one can be set, so we know
         * that the principal & keytab above are null.
         */
        final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE)
                .asControllerService(KerberosCredentialsService.class);
        if (credentialsService != null) {
            principal = credentialsService.getPrincipal();
            keytab = credentialsService.getKeytab();
        }


        String serviceName = context.getProperty(JAAS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(keytab) && StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(serviceName)) {
            mapToPopulate.put(SaslConfigs.SASL_JAAS_CONFIG, "com.sun.security.auth.module.Krb5LoginModule required "
                    + "useTicketCache=false "
                    + "renewTicket=true "
                    + "serviceName=\"" + serviceName + "\" "
                    + "useKeyTab=true "
                    + "keyTab=\"" + keytab + "\" "
                    + "principal=\"" + principal + "\";");
        }
    }

    private static Set<String> getPublicStaticStringFieldValues(final Class<?>... classes) {
        final Set<String> strings = new HashSet<>();
        for (final Class<?> classType : classes) {
            for (final Field field : classType.getDeclaredFields()) {
                if (Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers())
                        && field.getType().equals(String.class)) {
                    try {
                        strings.add(String.valueOf(field.get(null)));
                    } catch (IllegalArgumentException | IllegalAccessException ex) {
                        //ignore
                    }
                }
            }
        }
        return strings;
    }

    private static boolean isStaticStringFieldNamePresent(final String name, final Class<?>... classes) {
        return getPublicStaticStringFieldValues(classes).contains(name);
    }

    private KafkaProcessorUtility() {

    }

    public static final class KafkaConfigValidator implements Validator {

        final Class<?> classType;

        public KafkaConfigValidator(final Class<?> classType) {
            this.classType = classType;
        }

        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            final boolean knownValue = isStaticStringFieldNamePresent(subject, classType,
                    CommonClientConfigs.class, SslConfigs.class, SaslConfigs.class);
            return new ValidationResult.Builder()
                    .subject(subject)
                    .explanation("Must be a known configuration parameter for this kafka client")
                    .valid(knownValue)
                    .build();
        }
    }
}
