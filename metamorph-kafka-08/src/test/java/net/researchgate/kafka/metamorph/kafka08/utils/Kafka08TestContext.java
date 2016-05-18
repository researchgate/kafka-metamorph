package net.researchgate.kafka.metamorph.kafka08.utils;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static scala.collection.JavaConversions.asScalaBuffer;

public class Kafka08TestContext implements Closeable {

    private static int sequence = 0;
    private final int brokerId;

    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private int port;
    private KafkaServer kafkaServer;
    private boolean initialized;

    public Kafka08TestContext() {
        brokerId = sequence++;
    }

    public void initialize() {
        if (initialized) {
            throw new IllegalStateException("Context has been already initialized");
        }
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        zkClient = new ZkClient(zkServer.connectString(), 10000, 10000, ZKStringSerializer$.MODULE$);

        port = TestUtils.choosePort();

        KafkaConfig config = new KafkaConfig(TestUtils.createBrokerConfig(brokerId, port, true));
        Time mock = new MockTime();

        kafkaServer = new KafkaServer(config, mock);
        kafkaServer.startup();

        initialized = true;
    }

    public void createTopic(String topic, int numPartitions) {
        createTopic(topic, numPartitions, 5000);
    }

    public void createTopic(String topic, int numPartitions, long timeout) {
        ensureInitialized();
        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(
                new String[] {"--topic", topic, "--partitions", String.valueOf(numPartitions),"--replication-factor", "1"}
        ));

        List<KafkaServer> servers = new ArrayList<>();
        servers.add(kafkaServer);

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            TestUtils.waitUntilMetadataIsPropagated(asScalaBuffer(servers), topic, partitionId, timeout);
        }
    }

    public String getBootstrapServerString() {
        ensureInitialized();
        return "localhost:" + port;
    }

    public KafkaProducer<String, String> createProducer() {
        return createProducer(StringSerializer.class, StringSerializer.class);
    }

    public <K,V> KafkaProducer<K,V> createProducer(Class keySerializerClass , Class valueSerializerClass) {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServerString());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        return new KafkaProducer<>(props);
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Context must be initialized, ensure that Kafka08TestContext:initialize() has been invoked");
        }
    }

    @Override
    public void close() throws IOException {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
        if (zkClient != null) {
            zkClient.close();
        }
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }
}
