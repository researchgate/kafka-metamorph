package net.researchgate.kafka.metamorph;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

/**
 * A Kafka consumer interface for consumption of a topic partition
 * <p>
 *     It allows the discovery of available partitions of a given topic and the ability to bind to exactly one topic
 *     partition to consume data from it. The interface allows full control over the offset but also allows the
 *     underlying implementation to handle offset control autonomously while iterating over the records.
 * </p>
 * <p>
 *     This interface is heavily inspired by the new unified consumer interface introduced within Kafka 0.9.0 but is
 *     backwards compatible to older Kafka version since it relies on its own data representations. Also it allows
 *     only a subset of operations (e.g. only possible to consume one partition per consumer instance) which simplifies
 *     the concrete implementation in older Kafka versions.
 * </p>
 */
public interface PartitionConsumer<K, V> extends Closeable {

    /**
     * Get partitions for a given topic
     *
     * @param topic The topic to get partitions for
     *
     * @return The list of partitions
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    Collection<TopicPartition> partitionsFor(String topic) throws PartitionConsumerException;

    /**
     * Manually assign a partition to this consumer. This interface allows to bind to only one partition at a time.
     *
     * @param partition The partition to assign this consumer
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    void assign(TopicPartition partition) throws PartitionConsumerException;

    /**
     * Fetch data for the assigned partition. It is an error if the consumer is not assigned to any partition before polling.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(long)}
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *            If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *            Must not be negative.
     *
     * @return Fetched records for the assigned partition
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    List<PartitionConsumerRecord<K, V>> poll(int timeout) throws PartitionConsumerException;

    /**
     * Get the offset of the next record that will be fetched for the assigned partition (if a record with that offset exists).
     *
     * It is an error if the consumer is not assigned to a partition before invoking this method.
     *
     * @return The offset
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    long position() throws PartitionConsumerException;

    /**
     * Get the earliest offset for the assigned partition
     *
     * It is an error if the consumer is not assigned to a partition before invoking this method.
     *
     * @return The offset
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    long earliestPosition() throws PartitionConsumerException;

    /**
     * Get the latest offset for the assigned partition
     *
     * It is an error if the consumer is not assigned to a partition before invoking this method.
     *
     * @return The offset
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    long latestPosition() throws PartitionConsumerException;

    /**
     * Explicitly sets the fetch offset which will be used on the next {@link #poll(int) poll(timeout)}
     *
     * It is an error if the consumer is not assigned to a partition before invoking this method.
     *
     * @param offset the offset which will be used on the next {@link #poll(int) poll(timeout)}
     *
     * @throws PartitionConsumerException when an unrecoverable error occurs (e.g. no broker available)
     */
    void seek(long offset) throws PartitionConsumerException;

    /**
     * Close the consumer
     */
    void close();
}
