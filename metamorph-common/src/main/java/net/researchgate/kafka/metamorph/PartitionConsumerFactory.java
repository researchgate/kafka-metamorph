package net.researchgate.kafka.metamorph;

import java.util.Iterator;
import java.util.Properties;
import java.util.ServiceLoader;

public abstract class PartitionConsumerFactory {

    public abstract <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer);

    public static PartitionConsumerFactory getInstance() {
        final Iterator<PartitionConsumerFactory> providers = ServiceLoader.load(PartitionConsumerFactory.class).iterator();
        if (providers.hasNext()) {
            return providers.next();
        }
        throw new IllegalStateException("No implementation selected, include 'metamorph-kafka-08/09/010' as a dependency");
    }
}
