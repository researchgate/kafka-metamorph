package net.researchgate.kafka.metamorph;

/**
 * A representation of a record which has been fetched from a specific topic and partition
 */
public class PartitionConsumerRecord<K,V> {
    private final String topic;
    private final int partition;
    private final K key;
    private final V value;
    private final long offset;

    /**
     * Creates a record to be received from a specified topic and partition
     *
     * @param topic     The topic this record is received from
     * @param partitionId The partition of the topic this record is received from
     * @param key       The record key
     * @param value     The record value
     * @param offset    The offset of this record in the corresponding Kafka partition
     */
    public PartitionConsumerRecord(String topic, int partitionId, K key, V value, long offset) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null");
        }
        this.topic = topic;
        this.partition = partitionId;
        this.key = key;
        this.value = value;
        this.offset = offset;
    }

    /**
     * @return the topic this record is received from
     */
    public String topic() {
        return topic;
    }

    /**
     * @return the partition from which this record is received
     */
    public int partition() {
        return partition;
    }

    /**
     * @return the key (or null if no key is specified) of the record
     */
    public K key() {
        return key;
    }

    /**
     * @return the value of the record
     */
    public V value() {
        return value;
    }

    /**
     * @return the position of this record in the corresponding partition.
     */
    public long offset() {
        return offset;
    }
}
