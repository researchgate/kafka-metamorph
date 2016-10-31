package net.researchgate.kafka.metamorph;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Implementation-agnostic factory for new {@link PartitionConsumer} instance creation.
 * <p>
 *     This factory returns implementation-specific provider which implements {@link PartitionConsumerProvider} interface.
 * </p>
 */
public class PartitionConsumerFactory {
    /**
     * Get implementation-specific provider which is able to create {@link PartitionConsumer}
     *
     * @return partition consumer provider
     */
    public PartitionConsumerProvider getProvider() {
        final Iterator<PartitionConsumerProvider> providers = ServiceLoader.load(PartitionConsumerProvider.class).iterator();
        if (providers.hasNext()) {
            return providers.next();
        }
        throw new IllegalStateException("No implementation selected, include 'metamorph-kafka-{version}' as a dependency");
    }
}
