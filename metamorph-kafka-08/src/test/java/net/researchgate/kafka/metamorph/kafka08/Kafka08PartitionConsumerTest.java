package net.researchgate.kafka.metamorph.kafka08;

import junit.framework.Assert;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import net.researchgate.kafka.metamorph.PartitionConsumer;
import net.researchgate.kafka.metamorph.TopicPartition;
import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static scala.collection.JavaConversions.asScalaBuffer;

public class Kafka08PartitionConsumerTest {

    private final static int BROKER_ID = 0;

    @Test
    public void testPartitionDiscovery() throws PartitionConsumerException {
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        ZkClient zkClient = new ZkClient(zkServer.connectString(), 10000, 10000, ZKStringSerializer$.MODULE$);

        int port = TestUtils.choosePort();
        Properties brokerProps = TestUtils.createBrokerConfig(BROKER_ID, port, true);

        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();

        KafkaServer kafkaServer = new KafkaServer(config, mock);
        kafkaServer.startup();

        final String topic = "test_topic";

        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(
            new String[] {"--topic", topic, "--partitions", "1","--replication-factor", "1"}
        ));

        List<KafkaServer> servers = new ArrayList<>();
        servers.add(kafkaServer);

        TestUtils.waitUntilMetadataIsPropagated(asScalaBuffer(servers), topic, 0, 5000);

        Kafka08PartitionConsumerConfig consumerConfig = new Kafka08PartitionConsumerConfig.Builder("localhost:" + port).build();
        PartitionConsumer<String, String> consumer = new Kafka08PartitionConsumer<>(consumerConfig);

        Collection<TopicPartition> partitions = consumer.partitionsFor(topic);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(0, ((TopicPartition) partitions.toArray()[0]).partition());

        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
}
