package net.researchgate.kafka.metamorph.kafka010;

import net.researchgate.kafka.metamorph.AbstractKafkaPartitionConsumerTest;
import net.researchgate.kafka.metamorph.KafkaTestContext;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.kafka010.utils.Kafka010TestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Kafka010PartitionConsumerTest extends AbstractKafkaPartitionConsumerTest {

    @Override
    protected KafkaTestContext getContext() {
        return new Kafka010TestContext();
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

    @Override
    protected void produceMessagesOrdered(String topic, int messageNum) throws ExecutionException, InterruptedException {
        try (KafkaProducer<String, String> producer = createProducer()) {
            for (int i = 0; i < messageNum; i++) {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
                future.get();
            }
        }
    }

    @Override
    protected void produceMessagesUnordered(String topic, int messageNum) throws ExecutionException, InterruptedException {
        try (KafkaProducer<String, String> producer = createProducer()) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();

            for (int i = 0; i < messageNum; i++) {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
                futures.add(future);
            }

            for (Future f : futures) {
                f.get();
            }
        }
    }

    @Override
    protected PartitionConsumer<String,String> initializeUnitUnderTest() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBootstrapServerString());
        return new Kafka010PartitionConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    @Ignore(value = "Not suitable for kafka 010")
    @Override
    @Test
    public void testPollBatched() throws Exception {
        super.testPollBatched();
    }

}
