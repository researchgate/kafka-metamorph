package net.researchgate.kafka.metamorph.kafka08;

import junit.framework.Assert;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
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
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());
    }

    @Test
    public void testPartitionDiscoveryMultiplePartitions() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 10);

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));

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
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));

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
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(5L, consumer.latestPosition());
    }

    @Test
    public void testPoll() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        KafkaProducer<String, String> producer = context.createProducer();

        for (int i = 0; i < 5; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
            future.get();
        }

        producer.close();

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));

        consumer.assign(new TopicPartition(topic, 0));

        List<PartitionConsumerRecord<String,String>> records = consumer.poll(0);
        Assert.assertEquals(5, records.size());
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(i, records.get(i).offset());
            Assert.assertEquals("test-key-" + i, records.get(i).key());
            Assert.assertEquals("test-value-" + i, records.get(i).value());
            Assert.assertEquals(topic, records.get(i).topic());
            Assert.assertEquals(0, records.get(i).partition());
        }
    }

    @Test
    public void testSeekAndPoll() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        KafkaProducer<String, String> producer = context.createProducer();

        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
            future.get();
        }

        producer.close();

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));

        consumer.assign(new TopicPartition(topic, 0));

        List<PartitionConsumerRecord<String,String>> records;

        consumer.seek(50L);
        records = consumer.poll(0);
        Assert.assertEquals(50, records.size());
        Assert.assertEquals(50L, records.get(0).offset());

        consumer.seek(consumer.earliestPosition());
        records = consumer.poll(0);
        Assert.assertEquals(100, records.size());
        Assert.assertEquals(0L, records.get(0).offset());

        consumer.seek(consumer.latestPosition());
        records = consumer.poll(0);
        Assert.assertEquals(0, records.size());
    }
}
