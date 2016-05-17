package net.researchgate.kafka.metamorph;

/**
 * A representation of a Kafka node
 */
public class KafkaNode {

    private String host;
    private int port;

    public KafkaNode(String host, int port) {
        if (host == null) {
            throw new IllegalArgumentException("Host must not be null");
        }
        this.host = host;
        this.port = port;
    }

    /**
     * @return the hostname of this node
     */
    public String host() {
        return host;
    }

    /**
     * @return the port of this node
     */
    public int port() {
        return port;
    }

    /**
     * A convenient factory method to create a KafkaNode from a string representation.
     * The string has to follow the format of "{hostname}:{port}"
     *
     * @param s The string which represents a Kafka node
     * @return The KafkaNode represented by the input string
     */
    public static KafkaNode fromString(String s) {
        final String[] kv = s.split(":");
        if (kv.length != 2) {
            throw new IllegalArgumentException(String.format("Cannot create instance from string '%s'", s));
        }
        return new KafkaNode(kv[0], Integer.parseInt(kv[1]));
    }

    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KafkaNode that = (KafkaNode) o;

        if (port != that.port) return false;
        return host.equals(that.host);

    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }
}
