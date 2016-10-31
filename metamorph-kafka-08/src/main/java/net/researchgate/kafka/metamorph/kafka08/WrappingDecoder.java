package net.researchgate.kafka.metamorph.kafka08;

import kafka.serializer.Decoder;
import org.apache.kafka.common.serialization.Deserializer;

public class WrappingDecoder<T> implements Decoder<T> {
    private final Deserializer<T> deserializer;

    public WrappingDecoder(Deserializer<T> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public T fromBytes(byte[] bytes) {
        return deserializer.deserialize(null, bytes);
    }
}
