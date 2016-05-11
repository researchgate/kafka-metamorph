package net.researchgate.kafka.metamorph.kafka08;

import net.researchgate.kafka.metamorph.KafkaNode;

import java.util.ArrayList;
import java.util.List;

public class Kafka08PartitionConsumerConfig {

    private final List<KafkaNode> bootstrapNodes;
    private final int bufferSize;
    private final int socketTimeoutMs;

    private Kafka08PartitionConsumerConfig(
            List<KafkaNode> bootstrapNodes,
            int socketTimeoutMs,
            int bufferSize
    ) {
        this.bootstrapNodes = bootstrapNodes;
        this.socketTimeoutMs = socketTimeoutMs;
        this.bufferSize = bufferSize;
    }

    public List<KafkaNode> bootstrapNodes() {
        return bootstrapNodes;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public int socketTimeoutMs() {
        return socketTimeoutMs;
    }

    public static class Builder {
        private List<KafkaNode> bootstrapNodes = new ArrayList<>();
        private int bufferSize = 64 * 1024;
        private int socketTimeoutMs = 100000;

        public Builder() {}

        public Builder(String bootstrapServers) {
            bootstrapServers(bootstrapServers);
        }

        public Builder bootstrapServers(String bootstrapServers) {
            bootstrapNodes = parseKafkaBootstrapServers(bootstrapServers);
            return this;
        }

        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder socketTimeoutMs(int socketTimeoutMs) {
            this.socketTimeoutMs = socketTimeoutMs;
            return this;
        }

        public Kafka08PartitionConsumerConfig build() {
            if (bootstrapNodes.isEmpty()) {
                throw new IllegalArgumentException("bootstrapNodes must not be empty");
            }
            if (socketTimeoutMs <= 0) {
                throw new IllegalArgumentException("socketTimeoutMs must be greater than 0");
            }
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("bufferSize must be greater than 0");
            }
            return new Kafka08PartitionConsumerConfig(
                    bootstrapNodes,
                    socketTimeoutMs,
                    bufferSize
            );
        }

        private static List<KafkaNode> parseKafkaBootstrapServers(String bootstrapServers) {
            final List<KafkaNode> servers = new ArrayList<>();
            final String[] splitted = bootstrapServers.split(",");
            for (String serverString : splitted) {
                servers.add(KafkaNode.fromString(serverString.trim()));
            }
            return servers;
        }
    }
}
