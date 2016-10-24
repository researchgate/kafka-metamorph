package net.researchgate.kafka.metamorph.kafka08;

import net.researchgate.kafka.metamorph.PartitionConsumer;

import java.util.Properties;

public class PartitionConsumerFactory extends net.researchgate.kafka.metamorph.PartitionConsumerFactory {

    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer) {
        return Kafka08PartitionConsumer.create(properties, keyDeserializer, valueDeserializer);
    }
}
