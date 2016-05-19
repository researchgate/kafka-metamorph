package net.researchgate.kafka.metamorph.kafka08;

import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import net.researchgate.kafka.metamorph.AbstractKafkaPartitionConsumerTest;
import net.researchgate.kafka.metamorph.KafkaTestContext;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.PartitionConsumerRecord;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka08.utils.Kafka08TestContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Kafka08PartitionConsumerTest extends AbstractKafkaPartitionConsumerTest {

    @Override
    protected KafkaTestContext getContext() {
        return new Kafka08TestContext();
    }

    @Override
    protected PartitionConsumer<String, String> initializeUnitUnderTest() {
        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        return new Kafka08PartitionConsumer<>(consumerConfig, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));
    }

    private KafkaProducer<String, String> createProducer() {
        return createProducer(StringSerializer.class, StringSerializer.class);
    }

    private <K,V> KafkaProducer<K,V> createProducer(Class keySerializerClass , Class valueSerializerClass) {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBootstrapServerString());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        return new KafkaProducer<>(props);
    }

    @Test
    public void testPartitionDiscoveryOnePartition() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());
    }

    @Test
    public void testPartitionDiscoveryMultiplePartitions() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 10);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(10, partitions.size());
        Assert.assertTrue(partitions.contains(new TopicPartition(topic, 0)));
        Assert.assertTrue(partitions.contains(new TopicPartition(topic, 9)));
    }

    @Test
    public void testFetchEmptyBoundaryOffsets() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(0L, consumer.latestPosition());
    }

    @Test
    public void testFetchBoundaryOffsets() throws PartitionConsumerException, ExecutionException, InterruptedException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        KafkaProducer<String, String> producer = createProducer();

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value"));
            futures.add(future);
        }

        for (Future f : futures) {
            f.get();
        }

        producer.close();

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(5L, consumer.latestPosition());
    }

    @Test
    public void testPoll() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        KafkaProducer<String, String> producer = createProducer();

        for (int i = 0; i < 5; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
            future.get();
        }

        producer.close();

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

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

        KafkaProducer<String, String> producer = createProducer();

        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
            future.get();
        }

        producer.close();

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        List<PartitionConsumerRecord<String,String>> records;

        consumer.seek(50);
        records = consumer.poll(0);
        Assert.assertEquals(50, records.size());
        Assert.assertEquals(50, records.get(0).offset());

        consumer.seek(consumer.earliestPosition());
        records = consumer.poll(0);
        Assert.assertEquals(100, records.size());
        Assert.assertEquals(0, records.get(0).offset());

        consumer.seek(consumer.latestPosition());
        records = consumer.poll(0);
        Assert.assertEquals(0, records.size());
    }

    @Test
    public void testPollBatched() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        KafkaProducer<String, String> producer = createProducer();

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < 7000; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value"));
            futures.add(future);
        }

        for (Future f : futures) {
            f.get();
        }

        producer.close();

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));
        List<PartitionConsumerRecord<String,String>> records = consumer.poll(0);
        Assert.assertEquals(0L, records.get(0).offset());
        Assert.assertEquals(1359, records.get(records.size() - 1).offset());
        long offset = 1359;
        while (offset < 6707) {
            records = consumer.poll(0);
            Assert.assertEquals(offset + 1, records.get(0).offset());
            Assert.assertEquals(offset + 1337, records.get(records.size() - 1).offset());
            offset += 1337;
        }
        records = consumer.poll(0);
        Assert.assertEquals(6708, records.get(0).offset());
        Assert.assertEquals(6999, records.get(records.size() - 1).offset());
    }

    @Test
    public void testPollTailing() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        Thread producerThread = new Thread() {
            public void run() {
                try (KafkaProducer<String, String> producer = createProducer()) {
                    Thread.sleep(10);

                    List<Future<RecordMetadata>> futures = new ArrayList<>();
                    for (int i = 0; i < 500; i++) {
                        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value"));
                        futures.add(future);
                    }

                    for (Future f : futures) {
                        f.get();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        };

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        List<PartitionConsumerRecord<String,String>> records = consumer.poll(10);
        Assert.assertEquals(0, records.size());

        producerThread.start();
        records = consumer.poll(300);
        producerThread.join();

        Assert.assertEquals(500, records.size());
    }

}
