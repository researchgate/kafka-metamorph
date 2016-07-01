package net.researchgate.kafka.metamorph.exceptions;

/**
 * Base class of all consumer exceptions
 */
public class PartitionConsumerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PartitionConsumerException() {
        super();
    }

    public PartitionConsumerException(String message) {
        super(message);
    }

    public PartitionConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartitionConsumerException(Throwable cause) {
        super(cause);
    }
}
