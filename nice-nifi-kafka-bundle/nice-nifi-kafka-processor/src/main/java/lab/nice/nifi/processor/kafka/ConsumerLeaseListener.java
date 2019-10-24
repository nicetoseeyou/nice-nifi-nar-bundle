package lab.nice.nifi.processor.kafka;

/**
 * Consumer lease listener
 */
public interface ConsumerLeaseListener {
    /**
     * indicate whether the lease is reusable
     *
     * @return true if the lease is reusable, otherwise false
     */
    boolean reusable();

    /**
     * process before closing the consumer in the lease
     */
    void beforeConsumerClose();
}
