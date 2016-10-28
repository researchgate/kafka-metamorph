package net.researchgate.kafka.metamorph;

import java.util.Properties;

public interface PartitionConsumerProvider {
    <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer);
    <K, V> PartitionConsumer<K, V> createConsumer(Properties properties);
}
