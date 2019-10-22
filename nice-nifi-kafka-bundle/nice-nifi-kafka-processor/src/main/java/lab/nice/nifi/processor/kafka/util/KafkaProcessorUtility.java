package lab.nice.nifi.processor.kafka.util;

public final class KafkaProcessorUtility {
    private KafkaProcessorUtility() {

    }

    public static String buildKafkaTransitURI(final String securityProtocol, final String brokers, final String topic) {
        StringBuilder builder = new StringBuilder();
        builder.append(securityProtocol);
        builder.append("://");
        builder.append(brokers);
        builder.append("/");
        builder.append(topic);
        return builder.toString();
    }
}
