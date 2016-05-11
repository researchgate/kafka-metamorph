package net.researchgate.kafka.metamorph.kafka08;

import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;

import java.util.Collection;
import java.util.List;

public class Kafka08PartitionConsumer<K,V> implements PartitionConsumer<K,V> {

    private final Kafka08PartitionConsumerConfig consumerConfig;

    public Kafka08PartitionConsumer(Kafka08PartitionConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    @Override
    public Collection<TopicPartition> partitionsFor(String topic) throws PartitionConsumerException {
        return null;
    }

    @Override
    public void assign(TopicPartition partition) throws PartitionConsumerException {

    }

    @Override
    public List<PartitionConsumerRecord<K, V>> poll(long timeout) throws PartitionConsumerException {
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

    }
}
