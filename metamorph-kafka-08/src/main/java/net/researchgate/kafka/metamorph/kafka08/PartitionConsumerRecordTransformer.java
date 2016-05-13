package net.researchgate.kafka.metamorph.kafka08;

import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;

import java.nio.ByteBuffer;

class PartitionConsumerRecordTransformer<K,V> {

    private final Decoder<K> keyDecoder;
    private final Decoder<V> valueDecoder;

    public PartitionConsumerRecordTransformer(Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
    }

    public PartitionConsumerRecord<K, V> transform(TopicPartition topicPartition, MessageAndOffset messageAndOffset) {
        Message message = messageAndOffset.message();
        byte[] keyBytes = toBytes(message.key());
        byte[] valueBytes = toBytes(message.payload());

        return new PartitionConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                decodeNullable(keyDecoder, keyBytes),
                decodeNullable(valueDecoder, valueBytes),
                messageAndOffset.offset()
        );
    }

    private byte[] toBytes(final ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        return bytes;
    }

    private <T> T decodeNullable(Decoder<T> decoder, byte[] bytes) {
        return bytes == null ? null : decoder.fromBytes(bytes);
    }
}
