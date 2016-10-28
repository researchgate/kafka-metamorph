package net.researchgate.kafka.metamorph.kafka09;

import net.researchgate.kafka.metamorph.PartitionConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;

public class PartitionConsumerProvider implements net.researchgate.kafka.metamorph.PartitionConsumerProvider {

    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer) {
        // TODO: cast decoder/deserializer instances and re-throw exceptions with sane messages
        return new Kafka09PartitionConsumer<>(properties, (Deserializer<K>) keyDeserializer, (Deserializer<V>) valueDeserializer);
    }

    @Override
    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties) {
        return new Kafka09PartitionConsumer<>(properties);
    }
}
