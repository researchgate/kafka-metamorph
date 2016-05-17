package net.researchgate.kafka.metamorph;

/**
 * A representation of a Kafka topic and partition
 */
public class TopicPartition {

    private final int partition;
    private final String topic;

    public TopicPartition(String topic, int partition) {
        this.partition = partition;
        this.topic = topic;
    }

    /**
     * @return the partition id
     */
    public int partition() {
        return partition;
    }

    /**
     * @return the topic name
     */
    public String topic() {
        return topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicPartition that = (TopicPartition) o;

        if (partition != that.partition) return false;
        return topic != null ? topic.equals(that.topic) : that.topic == null;
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}