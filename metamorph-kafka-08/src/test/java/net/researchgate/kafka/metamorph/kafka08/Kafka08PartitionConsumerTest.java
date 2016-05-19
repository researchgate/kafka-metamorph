package net.researchgate.kafka.metamorph.kafka08;

import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import net.researchgate.kafka.metamorph.AbstractKafkaPartitionConsumerTest;
import net.researchgate.kafka.metamorph.KafkaTestContext;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.kafka08.utils.Kafka08TestContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
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

    @Override
    protected void produceMessagesOrdered(String topic, int messageNum) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = createProducer();

        for (int i = 0; i < messageNum; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value-" + i));
            future.get();
        }

        producer.close();
    }

    @Override
    protected void produceMessagesUnordered(String topic, int messageNum) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = createProducer();

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < messageNum; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "test-key-" + i, "test-value"));
            futures.add(future);
        }

        for (Future f : futures) {
            f.get();
        }

        producer.close();
    }
}
