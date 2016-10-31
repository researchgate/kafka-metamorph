/**
 * Subset of Kafka config class
 */

package net.researchgate.kafka.metamorph;

/**
 * The consumer configuration keys
 */
public class ConsumerConfig {
    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /**
     * <code>max.partition.fetch.bytes</code>
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";

    /** <code>key.deserializer</code> */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";

    /** <code>value.deserializer</code> */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";

    /** <code>request.timeout.ms</code> */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;

}
