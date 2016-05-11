package net.researchgate.kafka.metamorph.kafka08.exceptions;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;

/**
 * Raised when no broker is available
 */
public class NoBrokerAvailableException extends PartitionConsumerException {

    private static final long serialVersionUID = 1L;

    public NoBrokerAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoBrokerAvailableException(String message) {
        super(message);
    }

    public NoBrokerAvailableException() {
        super();
    }

    public NoBrokerAvailableException(Throwable cause) {
        super(cause);
    }
}
