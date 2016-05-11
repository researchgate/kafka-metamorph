package net.researchgate.kafka.metamorph.kafka08;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import net.researchgate.kafka.metamorph.KafkaNode;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.NoBrokerAvailableException;

import java.util.*;

public class Kafka08PartitionConsumer<K,V> implements PartitionConsumer<K,V> {

    private final Kafka08PartitionConsumerConfig consumerConfig;

    public Kafka08PartitionConsumer(Kafka08PartitionConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    @Override
    public Collection<TopicPartition> partitionsFor(String topic) throws PartitionConsumerException {
        List<TopicMetadata> topicMetadataList = getTopicMetadataFromBroker(consumerConfig.bootstrapNodes(), topic);
        final Set<TopicPartition> partitions = new HashSet<>();

        for (TopicMetadata topicMeta : topicMetadataList) {
            for (PartitionMetadata partitionMeta : topicMeta.partitionsMetadata()) {
                partitions.add(new TopicPartition(topic, partitionMeta.partitionId()));
            }
        }

        return partitions;
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

    private List<TopicMetadata> getTopicMetadataFromBroker(List<KafkaNode> bootstrapNodes, String topicName) throws NoBrokerAvailableException {
        Exception lastException = null;
        for (KafkaNode bootstrapNode : bootstrapNodes) {
            SimpleConsumer consumer = null;

            try {
                consumer = createConsumer(bootstrapNode);
                final TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(topicName));
                final TopicMetadataResponse response = consumer.send(request);

                return response.topicsMetadata();
            } catch (Exception e) {
                lastException = e;
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }

        final String message = String.format("No broker available for topic '%s' with servers '%s'", topicName, Arrays.toString(bootstrapNodes.toArray()));
        throw new NoBrokerAvailableException(message, lastException);
    }

    private SimpleConsumer createConsumer(KafkaNode node) {
        // @todo proper clientId generation
        String clientIdPrefix = "Client";
        return new SimpleConsumer(node.host(), node.port(), consumerConfig.socketTimeoutMs(), consumerConfig.bufferSize(), clientIdPrefix);
    }
}
