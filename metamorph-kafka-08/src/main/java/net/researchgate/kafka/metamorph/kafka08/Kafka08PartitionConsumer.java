package net.researchgate.kafka.metamorph.kafka08;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import net.researchgate.kafka.metamorph.KafkaNode;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.NoBrokerAvailableException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.OffsetFetchException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.PartitionNotAvailableException;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.*;

public class Kafka08PartitionConsumer<K,V> implements PartitionConsumer<K,V> {

    private final Kafka08PartitionConsumerConfig consumerConfig;

    private SimpleConsumer partitionConsumer;
    private TopicPartition assignedTopicPartition;
    private boolean isClosed = false;

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
        assignedTopicPartition = partition;
        reinitializePartitionConsumer();
    }

    @Override
    public List<PartitionConsumerRecord<K, V>> poll(long timeout) throws PartitionConsumerException {
        ensureAssignedAndNotClosed();
        return null;
    }

    @Override
    public long position() throws PartitionConsumerException {
        ensureAssignedAndNotClosed();
        return 0;
    }

    @Override
    public long earliestPosition() throws PartitionConsumerException {
        ensureAssignedAndNotClosed();
        return getOffsetForPartition(partitionConsumer, kafka.api.OffsetRequest.EarliestTime());
    }

    @Override
    public long latestPosition() throws PartitionConsumerException {
        ensureAssignedAndNotClosed();
        return getOffsetForPartition(partitionConsumer, kafka.api.OffsetRequest.LatestTime());
    }

    @Override
    public void seek(long offset) throws PartitionConsumerException {
        ensureAssignedAndNotClosed();
    }

    @Override
    public void close() {
        if (partitionConsumer != null) {
            partitionConsumer.close();
            isClosed = true;
        }
    }

    private void ensureAssignedAndNotClosed() {
        if (partitionConsumer == null || isClosed) {
            throw new InvalidStateException("Consumer is not assigned to any partitions or connection has been already closed.");
        }
    }

    private void reinitializePartitionConsumer() throws NoBrokerAvailableException, PartitionNotAvailableException {
        // ensure that old connection is closed before opening a new one
        if (partitionConsumer != null) {
            partitionConsumer.close();
        }
        partitionConsumer = createConsumer(getPartitionLeader(consumerConfig.bootstrapNodes(), assignedTopicPartition.topic(), assignedTopicPartition.partition()));
    }

    private long getOffsetForPartition(SimpleConsumer consumer, long whichTime) throws OffsetFetchException {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(new TopicAndPartition(assignedTopicPartition.topic(), assignedTopicPartition.partition()), new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            throw new OffsetFetchException("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(assignedTopicPartition.topic(), assignedTopicPartition.partition()));
        }
        long[] offsets = response.offsets(assignedTopicPartition.topic(), assignedTopicPartition.partition());
        return offsets[0];
    }

    private KafkaNode getPartitionLeader(List<KafkaNode> bootstrapNodes, String topicName, int partitionId) throws NoBrokerAvailableException, PartitionNotAvailableException {
        PartitionMetadata partitionMetadata = getMetadataForPartition(bootstrapNodes, topicName, partitionId);
        Broker leader = partitionMetadata.leader();
        return new KafkaNode(leader.host(), leader.port());
    }

    private PartitionMetadata getMetadataForPartition(List<KafkaNode> bootstrapNodes, String topicName, int partitionId) throws NoBrokerAvailableException, PartitionNotAvailableException {
        List<TopicMetadata> topicMetadataList = getTopicMetadataFromBroker(bootstrapNodes, topicName);

        for (TopicMetadata topicMeta : topicMetadataList) {
            for (PartitionMetadata partitionMeta : topicMeta.partitionsMetadata()) {
                if (partitionMeta.partitionId() == partitionId) {
                    return partitionMeta;
                }
            }
        }

        throw new PartitionNotAvailableException(String.format("Cannot find partition '%s' for topic '%s' with servers '%s'",
                partitionId, topicName, Arrays.toString(bootstrapNodes.toArray())));
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
