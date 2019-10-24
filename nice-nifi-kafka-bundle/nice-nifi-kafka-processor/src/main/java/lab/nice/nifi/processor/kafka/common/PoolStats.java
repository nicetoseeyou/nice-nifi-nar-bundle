package lab.nice.nifi.processor.kafka.common;

import java.util.Objects;

public final class PoolStats {
    final long consumerCreatedCount;
    final long consumerClosedCount;
    final long leasesObtainedCount;

    public PoolStats(final long consumerCreatedCount, final long consumerClosedCount, final long leasesObtainedCount) {
        this.consumerCreatedCount = consumerCreatedCount;
        this.consumerClosedCount = consumerClosedCount;
        this.leasesObtainedCount = leasesObtainedCount;
    }

    public long getConsumerCreatedCount() {
        return consumerCreatedCount;
    }

    public long getConsumerClosedCount() {
        return consumerClosedCount;
    }

    public long getLeasesObtainedCount() {
        return leasesObtainedCount;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PoolStats poolStats = (PoolStats) o;
        return consumerCreatedCount == poolStats.consumerCreatedCount &&
                consumerClosedCount == poolStats.consumerClosedCount &&
                leasesObtainedCount == poolStats.leasesObtainedCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerCreatedCount, consumerClosedCount, leasesObtainedCount);
    }

    @Override
    public String toString() {
        return "PoolStats{" +
                "consumerCreatedCount=" + consumerCreatedCount +
                ", consumerClosedCount=" + consumerClosedCount +
                ", leasesObtainedCount=" + leasesObtainedCount +
                '}';
    }
}
