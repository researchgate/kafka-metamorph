package net.researchgate.kafka.metamorph;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public abstract class AbstractKafkaPartitionConsumerTest {

    protected KafkaTestContext context;

    protected abstract PartitionConsumer<String, String> initializeUnitUnderTest();


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
