package net.researchgate.kafka.metamorph.kafka010.utils;

import info.batey.kafka.unit.KafkaUnit;
import net.researchgate.kafka.metamorph.KafkaTestContext;

import java.io.IOException;

public class Kafka010TestContext implements KafkaTestContext {
    private final int kafkaPort;
    private final KafkaUnit kafkaUnit;

    private boolean initialized = false;

    public Kafka010TestContext() {
        int zkPort = NetUtils.getRandomPort();
        kafkaPort = NetUtils.getRandomPort();
        kafkaUnit = new KafkaUnit(zkPort, kafkaPort);
    }

    @Override
    public void initialize() {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        kafkaUnit.startup();
        initialized = true;
    }

    @Override
    public void createTopic(String topic, int numPartitions) {
        ensureInitialized();
        kafkaUnit.createTopic(topic, numPartitions);
    }

    @Override
    public void createTopic(String topic, int numPartitions, long timeout) {
        this.createTopic(topic, numPartitions);
    }

    @Override
    public String getBootstrapServerString() {
        return "localhost:" + kafkaPort;
    }

    @Override
    public void close() throws IOException {
        if (kafkaUnit != null) {
            kafkaUnit.shutdown();
        }
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Context was not initialized properly. Call KafkaTestContest.initialize() before");
        }
    }
}
