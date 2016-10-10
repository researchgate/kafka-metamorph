package net.researchgate.kafka.metamorph.kafka010;

import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.*;

public class Kafka010PartitionConsumer<K,V> implements PartitionConsumer<K,V> {

    private org.apache.kafka.common.TopicPartition assignedKafkaPartition;
    private final KafkaConsumer<K,V> consumer;

    public Kafka010PartitionConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        consumer = new KafkaConsumer<K, V>(properties, keyDeserializer, valueDeserializer);
    }

    @Override
    public Collection<TopicPartition> partitionsFor(String topic)  {
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
    public void assign(TopicPartition partition) {
        try {
            List<org.apache.kafka.common.TopicPartition> partitions = new ArrayList<>();
            assignedKafkaPartition = new org.apache.kafka.common.TopicPartition(partition.topic(), partition.partition());
            partitions.add(assignedKafkaPartition);
            consumer.assign(partitions);
        } catch (KafkaException e) {
            throw new PartitionConsumerException(e);
        }
    }

    @Override
    public List<PartitionConsumerRecord<K, V>> poll(int timeout)  {
        ensureAssigned();
        ConsumerRecords<K, V> consumerRecords = consumer.poll(timeout);
        List<PartitionConsumerRecord<K, V>> records = new ArrayList<>();
        for (ConsumerRecord<K, V> consumerRecord: consumerRecords) {
            records.add(new PartitionConsumerRecord<>(
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.offset()));
        }
        return records;
    }

    @Override
    public long position() {
        ensureAssigned();
        return consumer.position(assignedKafkaPartition);
    }

    @Override
    public long earliestPosition() {
        ensureAssigned();
        long currentPosition = consumer.position(assignedKafkaPartition);
        consumer.seekToBeginning(consumer.assignment());
        long earliestPosition = consumer.position(assignedKafkaPartition);
        consumer.seek(assignedKafkaPartition, currentPosition);
        return earliestPosition;
    }

    @Override
    public long latestPosition()  {
        ensureAssigned();
        long currentPosition = consumer.position(assignedKafkaPartition);
        consumer.seekToEnd(consumer.assignment());
        long latestPosition = consumer.position(assignedKafkaPartition);
        consumer.seek(assignedKafkaPartition, currentPosition);
        return latestPosition;
    }

    @Override
    public void seek(long offset)  {
        ensureAssigned();
        consumer.seek(assignedKafkaPartition, offset);
    }

    @Override
    public void close() {
        consumer.close();
    }

    private void ensureAssigned() {
        if (assignedKafkaPartition == null || consumer.assignment().isEmpty()) {
            throw new IllegalStateException("Consumer is not assigned to any partitions.");
        }
    }
}
