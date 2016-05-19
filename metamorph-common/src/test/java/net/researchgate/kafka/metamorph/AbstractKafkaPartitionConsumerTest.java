package net.researchgate.kafka.metamorph;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class AbstractKafkaPartitionConsumerTest {

    protected KafkaTestContext context;

    protected abstract PartitionConsumer<String, String> initializeUnitUnderTest();

    protected abstract KafkaTestContext getContext();

    protected abstract void produceMessagesOrdered(String topic, int messageNum) throws ExecutionException, InterruptedException;
    protected abstract void produceMessagesUnordered(String topic, int messageNum) throws ExecutionException, InterruptedException;

    @Before
    public void setUp() {
        context = getContext();
        context.initialize();
    }

    @After
    public void tearDown() throws IOException {
        context.close();
    }

    @Test
    public void testPartitionDiscoveryOnePartition() throws PartitionConsumerException {
        final String topic = "test-topic";
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

        produceMessagesUnordered(topic, 5);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(5L, consumer.latestPosition());
    }

    @Test
    public void testPoll() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        produceMessagesOrdered(topic, 5);

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

        produceMessagesOrdered(topic, 100);

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

        produceMessagesUnordered(topic, 7000);

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
                try {
                    Thread.sleep(10);
                    produceMessagesUnordered(topic, 500);
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
