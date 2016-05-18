package net.researchgate.kafka.metamorph.kafka09;

import net.researchgate.kafka.metamorph.AbstractKafkaPartitionConsumerTest;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.kafka09.utils.Kafka09TestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Properties;

public class Kafka09PartitionConsumerTest extends AbstractKafkaPartitionConsumerTest {

    @Before
    public void setUp() {
        context = new Kafka09TestContext();
        context.initialize();
    }

    @After
    public void tearDown() throws IOException {
        context.close();
    }

    @Override
    protected PartitionConsumer<String,String> initializeUnitUnderTest() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBootstrapServerString());
        return new Kafka09PartitionConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }
}
