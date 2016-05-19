package net.researchgate.kafka.metamorph;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public abstract class AbstractKafkaPartitionConsumerTest {

    protected KafkaTestContext context;

    protected abstract PartitionConsumer<String, String> initializeUnitUnderTest();

    protected abstract KafkaTestContext getContext();

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
}
