package net.researchgate.kafka.metamorph.kafka09;

import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.*;

public class Kafka09PartitionConsumer<K,V> implements PartitionConsumer<K,V> {

    private TopicPartition assignedPartition;
    private final KafkaConsumer<K,V> consumer;

    public Kafka09PartitionConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        consumer = new KafkaConsumer<K, V>(properties, keyDeserializer, valueDeserializer);
    }

    @Override
    public Collection<TopicPartition> partitionsFor(String topic) throws PartitionConsumerException {
        try {
            Collection<TopicPartition> partitions = new HashSet<>();
            for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
            return partitions;
        } catch (KafkaException e) {
            throw new PartitionConsumerException(e);
        }
    }

    @Override
    public void assign(TopicPartition partition) throws PartitionConsumerException {
        try {
            List<org.apache.kafka.common.TopicPartition> partitions = new ArrayList<>();
            partitions.add(new org.apache.kafka.common.TopicPartition(partition.topic(), partition.partition()));
            consumer.assign(partitions);
        } catch (KafkaException e) {
            throw new PartitionConsumerException(e);
        }
        assignedPartition = partition;
    }

    @Override
    public List<PartitionConsumerRecord<K, V>> poll(int timeout) throws PartitionConsumerException {
        return null;
    }

    @Override
    public long position() throws PartitionConsumerException {
        return 0;
    }

    @Override
    public long earliestPosition() throws PartitionConsumerException {
        return 0;
    }

    @Override
    public long latestPosition() throws PartitionConsumerException {
        return 0;
    }

    @Override
    public void seek(long offset) throws PartitionConsumerException {

    }

    @Override
    public void close() {
        consumer.close();
    }
}
