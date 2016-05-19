package net.researchgate.kafka.metamorph.kafka09;

import net.researchgate.kafka.metamorph.AbstractKafkaPartitionConsumerTest;
import net.researchgate.kafka.metamorph.KafkaTestContext;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.kafka09.utils.Kafka09TestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class Kafka09PartitionConsumerTest extends AbstractKafkaPartitionConsumerTest {

    @Override
    protected KafkaTestContext getContext() {
        return new Kafka09TestContext();
    }

    @Override
    protected PartitionConsumer<String,String> initializeUnitUnderTest() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBootstrapServerString());
        return new Kafka09PartitionConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

}
