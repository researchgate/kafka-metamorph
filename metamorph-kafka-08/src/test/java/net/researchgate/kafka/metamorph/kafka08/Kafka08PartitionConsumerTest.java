package net.researchgate.kafka.metamorph.kafka08;

import junit.framework.Assert;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka08.utils.KafkaTestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

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
    public void testPartitionDiscovery() throws PartitionConsumerException {
        final String topic = "test_topic";
        context.createTopic(topic, 1);

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder(context.getBootstrapServerString()).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig);

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());
    }
}
