package net.researchgate.kafka.metamorph.kafka08;

import kafka.serializer.Decoder;
import net.researchgate.kafka.metamorph.ConsumerConfig;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PartitionConsumerProvider implements net.researchgate.kafka.metamorph.PartitionConsumerProvider {

    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties, Object keyDeserializer, Object valueDeserializer) {
        Kafka08PartitionConsumerConfig config = getConfigFromProperties(properties);
        // TODO: cast decoder/deserializer instances and re-throw exceptions with sane messages
        return new Kafka08PartitionConsumer<>(config, (Decoder<K>) keyDeserializer, (Decoder<V>) valueDeserializer);
    }

    private static Kafka08PartitionConsumerConfig getConfigFromProperties(Properties properties) {
        Kafka08PartitionConsumerConfig.Builder builder = new Kafka08PartitionConsumerConfig.Builder();
        builder = builder.bootstrapServers(properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        String requestTimeout = properties.getProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        if (requestTimeout != null) {
            builder = builder.socketTimeoutMs(Integer.valueOf(requestTimeout, 10));
        }
        String maxMessageSize = properties.getProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        if (maxMessageSize != null) {
            builder = builder.bufferSize(Integer.valueOf(maxMessageSize, 10));
        }
        return builder.build();
    }

    public <K, V> PartitionConsumer<K, V> createConsumer(Properties properties) {
        Kafka08PartitionConsumerConfig config = getConfigFromProperties(properties);
        Deserializer<K> keyDeserializer = getDeserializer(properties, properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), true);
        Deserializer<V> valueDeserializer = getDeserializer(properties, properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), false);
        WrappingDecoder<K> wrappingKeyDecoder = new WrappingDecoder<K>(keyDeserializer);
        WrappingDecoder<V> wrappingValueDecoder = new WrappingDecoder<V>(valueDeserializer);
        return new Kafka08PartitionConsumer<>(config, wrappingKeyDecoder, wrappingValueDecoder);
    }

    private <T> Deserializer<T> getDeserializer(Properties properties, String className, boolean isKey) {
        Deserializer<T> deserializer = getConfiguredInstance(className, Deserializer.class);
        if (deserializer == null) {
            throw new PartitionConsumerException(String.format("Can't instantiate deserializer from %s", className));
        }
        Map<String, String> map = new HashMap<>();
        for (final String name: properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name));
        }
        deserializer.configure(map, isKey);
        return deserializer;
    }

    private static <T> T newInstance(Class<T> c) {
        try {
            return c.newInstance();
        } catch (IllegalAccessException e) {
            throw new PartitionConsumerException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new PartitionConsumerException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
        } catch (NullPointerException e) {
            throw new PartitionConsumerException("Requested class was null", e);
        }
    }

    private Class<?> getClass(String key) {
        try {
            return Class.forName(key, true, ClassLoader.getSystemClassLoader());
        } catch (ClassNotFoundException e) {
            throw new PartitionConsumerException("Class not found", e);
        }
    }

    private <T> T getConfiguredInstance(String key, Class<T> t) {
        Class<?> c = getClass(key);
        if (c == null)
            return null;
        Object o = newInstance(c);
        if (!t.isInstance(o))
            throw new PartitionConsumerException(c.getName() + " is not an instance of " + t.getName());
        return t.cast(o);
    }

}
