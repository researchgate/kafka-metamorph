package net.researchgate.kafka.metamorph.kafka08;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.network.BlockingChannel;
import kafka.serializer.Decoder;
import net.researchgate.kafka.metamorph.KafkaNode;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.NoBrokerAvailableException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.OffsetFetchException;
import net.researchgate.kafka.metamorph.kafka08.exceptions.PartitionNotAvailableException;

import java.util.*;

public class Kafka08PartitionConsumer<K, V> implements PartitionConsumer<K, V> {

    private final Kafka08PartitionConsumerConfig consumerConfig;
    private final PartitionConsumerRecordTransformer<K, V> recordTransformer;

    private SimpleConsumer partitionConsumer;
    private TopicPartition assignedTopicPartition;
    private long fetchOffset;
    private boolean isClosed = false;

    public Kafka08PartitionConsumer(Kafka08PartitionConsumerConfig consumerConfig, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        this.consumerConfig = consumerConfig;
        this.recordTransformer = new PartitionConsumerRecordTransformer<>(keyDecoder, valueDecoder);
    }

    @Override
    public Collection<TopicPartition> partitionsFor(String topic) {
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
    public void assign(TopicPartition partition) {
        assignedTopicPartition = partition;
        reinitializePartitionConsumer();
    }

    @Override
    public List<PartitionConsumerRecord<K, V>> poll(int timeout) {
        ensureAssignedAndNotClosed();
        ByteBufferMessageSet messageSet = getMessageSetSince(fetchOffset, timeout);
        List<PartitionConsumerRecord<K, V>> records = new ArrayList<>();
        for (MessageAndOffset messageAndOffset : messageSet) {
            // @todo handle serialization errors properly
            PartitionConsumerRecord<K, V> record = recordTransformer.transform(assignedTopicPartition, messageAndOffset);
            records.add(record);
            fetchOffset = Math.max(messageAndOffset.nextOffset(), fetchOffset);
        }
        return records;
    }

    @Override
    public long position() {
        ensureAssignedAndNotClosed();
        return fetchOffset;
    }

    @Override
    public long earliestPosition() {
        ensureAssignedAndNotClosed();
        return getOffsetForPartition(kafka.api.OffsetRequest.EarliestTime());
    }

    @Override
    public long latestPosition() {
        ensureAssignedAndNotClosed();
        return getOffsetForPartition(kafka.api.OffsetRequest.LatestTime());
    }

    @Override
    public void seek(long offset) {
        ensureAssignedAndNotClosed();
        if (offset < 0) {
            throw new IndexOutOfBoundsException(String.format("Negative offsets are disallowed, offset given: %d", offset));
        }
        fetchOffset = offset;
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
            throw new IllegalStateException("Consumer is not assigned to any partitions or connection has been already closed.");
        }
    }

    private void reinitializePartitionConsumer() throws NoBrokerAvailableException, PartitionNotAvailableException {
        // ensure that old connection is closed before opening a new one
        if (partitionConsumer != null) {
            partitionConsumer.close();
        }
        partitionConsumer = createConsumer(getPartitionLeader(consumerConfig.bootstrapNodes(), assignedTopicPartition.topic(), assignedTopicPartition.partition()));
    }

    private ByteBufferMessageSet getMessageSetSince(long offset, int timeoutInMs)  {
        if (timeoutInMs < 0) {
            throw new IllegalArgumentException(String.format("Timeout must not lower than 0, timeout is: %d", timeoutInMs));
        }
        FetchRequest request = new FetchRequestBuilder()
                .clientId(generateClientId())
                .addFetch(assignedTopicPartition.topic(), assignedTopicPartition.partition(), offset, consumerConfig.bufferSize())
                .maxWait(timeoutInMs)
                .minBytes(consumerConfig.bufferSize())
                .build();
        FetchResponse response = partitionConsumer.fetch(request);
        if (response.hasError()) {
            short errorCode = response.errorCode(assignedTopicPartition.topic(), assignedTopicPartition.partition());
            // @todo retry during broker failover
            throw new PartitionConsumerException(ErrorMapping.exceptionFor(errorCode));
        }
        return response.messageSet(assignedTopicPartition.topic(), assignedTopicPartition.partition());
    }

    private long getOffsetForPartition(long whichTime) throws OffsetFetchException {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(new TopicAndPartition(assignedTopicPartition.topic(), assignedTopicPartition.partition()), new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), partitionConsumer.clientId());
        OffsetResponse response = partitionConsumer.getOffsetsBefore(request);

        if (response.hasError()) {
            short errorCode = response.errorCode(assignedTopicPartition.topic(), assignedTopicPartition.partition());
            throw new OffsetFetchException("Error fetching data Offset Data the Broker. Error code: " + errorCode, ErrorMapping.exceptionFor(errorCode));
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

    private String generateClientId() {
        // @todo proper clientId generation
        return "Client";
    }

    private SimpleConsumer createConsumer(KafkaNode node) {
        return new SimpleConsumer(node.host(), node.port(), consumerConfig.socketTimeoutMs(), BlockingChannel.UseDefaultBufferSize(), generateClientId());
    }

    public static <K, V> PartitionConsumer<K, V> create(Properties properties, Object keyDecoder, Object valueDecoder) {
        Kafka08PartitionConsumerConfig config = new Kafka08PartitionConsumerConfig.Builder().bootstrapServers(properties.getProperty("bootstrap.servers")).build();
        // TODO: check instances, cast with K and V, avoid property name hardcoding
        return new Kafka08PartitionConsumer<K, V>(config, (Decoder) keyDecoder, (Decoder) valueDecoder);
    }
}
