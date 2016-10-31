package net.researchgate.kafka.metamorph;

import java.util.Properties;

/**
 * Interface to get configured instance of {@link PartitionConsumer} without explicit Kafka version specification.
 */
public interface PartitionConsumerProvider {
    /**
     * Creates consumer with key/value-decoder/deserializer instances passed.
     * Discouraged in favor of {@link #createConsumer(Properties)} which doesn't require to create deserializer instances.
     *
     * @param properties - consumer properties, see {@link ConsumerConfig} for the details.
     *                   For Kafka 08 only listed properties are supported, for greater versions all consumer properties.
     * @param keyDeserializer - implementation-specific decoder/deserializer for the key
     * @param valueDeserializer - implementation-specific decoder/deserializer for the key
     * @param <K> - key class
     * @param <V> - value class
     * @return configured partition consumer
     */
    <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer);

    /**
     * @param properties - consumer properties, see {@link ConsumerConfig} for the details
     * @param <K> - key class
     * @param <V> - value class
     * @return configured partition consumer with dynamically created and configured key/value deserializers
     */
    <K, V> PartitionConsumer<K, V> createConsumer(Properties properties);
}
