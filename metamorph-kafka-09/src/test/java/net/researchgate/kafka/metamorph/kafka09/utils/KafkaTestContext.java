package net.researchgate.kafka.metamorph.kafka09.utils;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Option;
import scala.Tuple2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static scala.collection.JavaConversions.asScalaBuffer;

public class KafkaTestContext implements Closeable {

    private static int sequence = 0;
    private final int brokerId;

    private EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;
    private boolean initialized;
    private ZkUtils zkUtils;

    public KafkaTestContext() {
        brokerId = sequence++;
    }

    public void initialize() {
        if (initialized) {
            throw new IllegalStateException("Context has been already initialized");
        }
        zkServer = new EmbeddedZookeeper();
        final int port = getRandomPort();

        Properties brokerConfig = TestUtils.createBrokerConfig(brokerId, "localhost:" + zkServer.port(), true, true, port, Option.<SecurityProtocol>empty(), Option.<File>empty(), true, false, 0, false, 0, false, 0);
        KafkaConfig config = new KafkaConfig(brokerConfig);
        Time mock = new MockTime();

        kafkaServer = new KafkaServer(config, mock, Option.<String>empty());
        kafkaServer.startup();

        zkUtils = ZkUtils.apply("127.0.0.1:" + zkServer.port(), 10000, 10000, false);

        initialized = true;
    }

    public void createTopic(String topic, int numPartitions) {
        createTopic(topic, numPartitions, 5000);
    }

    public void createTopic(String topic, int numPartitions, long timeout) {
        ensureInitialized();
        TopicCommand.createTopic(zkUtils, new TopicCommand.TopicCommandOptions(
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
        return kafkaServer.config().advertisedHostName() + ":" + kafkaServer.config().advertisedPort();
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
            throw new IllegalStateException("Context must be initialized, ensure that KafkaTestContext:initialize() has been invoked");
        }
    }

    private int getRandomPort() {
        ServerSocket s = null;
        try {
            s = new ServerSocket(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return s.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
        if (zkUtils != null) {
            zkUtils.close();
        }
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }
}
