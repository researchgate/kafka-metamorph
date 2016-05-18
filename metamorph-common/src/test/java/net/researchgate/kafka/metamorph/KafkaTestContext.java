package net.researchgate.kafka.metamorph;

import java.io.Closeable;

public interface KafkaTestContext extends Closeable {
    void initialize();
    void createTopic(String topic, int numPartitions);
    void createTopic(String topic, int numPartitions, long timeout);
    String getBootstrapServerString();
}
