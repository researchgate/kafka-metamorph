package net.researchgate.kafka.metamorph.kafka08;

import junit.framework.Assert;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka08.utils.KafkaTestContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Kafka08PartitionConsumerTest {

    private KafkaTestContext context;

    @Before
    public void setUp() {
        context = new KafkaTestContext();
        context.initialize();
    }

    @After
    public void tearDown() throws IOException {
        context.close();
    }

    @Test
    public void testPartitionDiscoveryOnePartition() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig);

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());
    }

    @Test
    public void testPartitionDiscoveryMultiplePartitions() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 10);

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig);

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(10, partitions.size());
        Assert.assertTrue(partitions.contains(new TopicPartition(topic, 0)));
        Assert.assertTrue(partitions.contains(new TopicPartition(topic, 9)));
    }

    @Test
    public void testFetchEmptyBoundaryOffsets() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig);

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(0L, consumer.latestPosition());
    }

    @Test
    public void testFetchBoundaryOffsets() throws PartitionConsumerException, ExecutionException, InterruptedException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        KafkaProducer<String, String> producer = context.createProducer();

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value"));
            futures.add(future);
        }

        for (Future f : futures) {
            f.get();
        }

        producer.close();

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig);

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(5L, consumer.latestPosition());
    }
}
