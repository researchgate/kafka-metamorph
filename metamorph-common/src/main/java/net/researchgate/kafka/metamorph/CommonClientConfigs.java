/**
 * Subset of Kafka config class
 */

package net.researchgate.kafka.metamorph;

/**
 * Some configurations shared by both producer and consumer
 */
public class CommonClientConfigs {

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
}
