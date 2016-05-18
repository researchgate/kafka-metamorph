package net.researchgate.kafka.metamorph.kafka09;

import junit.framework.Assert;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import net.researchgate.kafka.metamorph.kafka09.utils.KafkaTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

public class Kafka09PartitionConsumerTest {

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
        final String topic = "test-topic";
        context.createTopic(topic, 1);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBootstrapServerString());
        PartitionConsumer<String, String> consumer = new Kafka09PartitionConsumer<>(props, new StringDeserializer(), new StringDeserializer());

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());
    }
}
