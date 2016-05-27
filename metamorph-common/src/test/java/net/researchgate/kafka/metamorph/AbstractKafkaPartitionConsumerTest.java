package net.researchgate.kafka.metamorph;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.fail;

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

    @Test(timeout = 5000)
    public void testPartitionDiscoveryOnePartition() throws PartitionConsumerException {
        final String topic = "test-topic";
        context.createTopic(topic, 1);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());
    }

    @Test(timeout = 5000)
    public void testPartitionDiscoveryMultiplePartitions() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 10);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(10, partitions.size());
        Assert.assertTrue(partitions.contains(new TopicPartition(topic, 0)));
        Assert.assertTrue(partitions.contains(new TopicPartition(topic, 9)));
    }

    @Test(timeout = 5000, expected = IllegalStateException.class)
    public void testIllegalOperationWithoutAssignedPartition() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();
        consumer.poll(100);
    }

    @Test(timeout = 5000)
    public void testFetchEmptyBoundaryOffsets() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(0L, consumer.latestPosition());
    }

    @Test(timeout = 5000)
    public void testFetchBoundaryOffsets() throws PartitionConsumerException, ExecutionException, InterruptedException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        produceMessagesUnordered(topic, 5);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        Assert.assertEquals(0L, consumer.earliestPosition());
        Assert.assertEquals(5L, consumer.latestPosition());
    }

    @Test(timeout = 5000)
    public void testPoll() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        produceMessagesOrdered(topic, 5);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));
        consumer.seek(consumer.earliestPosition());

        long latestOffset = consumer.latestPosition();
        Assert.assertEquals(5, latestOffset);
        Assert.assertEquals(0, consumer.position());

        List<PartitionConsumerRecord<String,String>> records = new ArrayList<>();
        while (consumer.position() < latestOffset) {
            records.addAll(consumer.poll(10));
        }

        Assert.assertEquals(5, records.size());
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(i, records.get(i).offset());
            Assert.assertEquals("test-key-" + i, records.get(i).key());
            Assert.assertEquals("test-value-" + i, records.get(i).value());
            Assert.assertEquals(topic, records.get(i).topic());
            Assert.assertEquals(0, records.get(i).partition());
        }
    }

    @Test(timeout = 5000)
    public void testSeekAndPoll() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        produceMessagesOrdered(topic, 100);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        List<PartitionConsumerRecord<String,String>> records = new ArrayList<>();

        long latestOffset = consumer.latestPosition();
        Assert.assertTrue(latestOffset > 50);

        consumer.seek(50);
        while (consumer.position() < latestOffset) {
            records.addAll(consumer.poll(10));
        }
        Assert.assertEquals(50, records.size());
        Assert.assertEquals(50, records.get(0).offset());

        consumer.seek(consumer.earliestPosition());
        records = new ArrayList<>();
        while (consumer.position() < latestOffset) {
            records.addAll(consumer.poll(10));
        }
        Assert.assertEquals(100, records.size());
        Assert.assertEquals(0, records.get(0).offset());

        consumer.seek(consumer.latestPosition());
        records = consumer.poll(10);
        Assert.assertEquals(0, records.size());
    }

    // TODO: This test uses kafka 8 batch size specifics, needs to be re-written.
    @Test(timeout = 5000)
    public void testPollBatched() throws Exception {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        produceMessagesUnordered(topic, 7000);

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));
        List<PartitionConsumerRecord<String,String>> records = consumer.poll(10);
        Assert.assertNotEquals(0, records.size());
        Assert.assertEquals(0L, records.get(0).offset());
        Assert.assertEquals(1359, records.get(records.size() - 1).offset());
        long offset = 1359;
        while (offset < 6707) {
            records = consumer.poll(10);
            Assert.assertEquals(offset + 1, records.get(0).offset());
            Assert.assertEquals(offset + 1337, records.get(records.size() - 1).offset());
            offset += 1337;
        }
        records = consumer.poll(10);
        Assert.assertEquals(6708, records.get(0).offset());
        Assert.assertEquals(6999, records.get(records.size() - 1).offset());
    }

    @Test(timeout = 5000)
    public void testPollTailing() throws Exception {
        final int maxWaitInMillis = 500;
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    produceMessagesUnordered(topic, 500);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        };

        PartitionConsumer<String, String> consumer = initializeUnitUnderTest();

        consumer.assign(new TopicPartition(topic, 0));

        List<PartitionConsumerRecord<String,String>> records;
        records = consumer.poll(10);
        Assert.assertEquals(0, records.size());

        producerThread.start();

        // This code tries to emulate 'tailing' behavior.
        // In usual use-case you would do this in a different fashion
        // Here we just want to ensure that the same consumer is able to fetch newly written data
        records = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            records.addAll(consumer.poll(10));
            if (records.size() == 500) {
                break;
            }
            if (System.currentTimeMillis() - start > maxWaitInMillis) {
                fail(String.format("Unable to consume topic within %d millis", maxWaitInMillis));
            }
        }
        producerThread.join();

        Assert.assertEquals(500, records.size());
    }
}
