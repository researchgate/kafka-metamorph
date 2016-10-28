package net.researchgate.kafka.metamorph;

import java.util.Iterator;
import java.util.ServiceLoader;

public class PartitionConsumerFactory {
    public PartitionConsumerProvider getProvider() {
        final Iterator<PartitionConsumerProvider> providers = ServiceLoader.load(PartitionConsumerProvider.class).iterator();
        if (providers.hasNext()) {
            return providers.next();
        }
        throw new IllegalStateException("No implementation selected, include 'metamorph-kafka-{version}' as a dependency");
    }
}
