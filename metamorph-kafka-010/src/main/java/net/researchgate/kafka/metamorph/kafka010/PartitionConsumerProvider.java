package net.researchgate.kafka.metamorph.kafka010;

import net.researchgate.kafka.metamorph.PartitionConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;

public class PartitionConsumerProvider implements net.researchgate.kafka.metamorph.PartitionConsumerProvider {

    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer) {
        // TODO: check instances
        return new Kafka010PartitionConsumer<>(properties, (Deserializer<K>) keyDeserializer, (Deserializer<V>) valueDeserializer);
    }

    @Override
    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties) {
        return new Kafka010PartitionConsumer<>(properties);
    }
}
