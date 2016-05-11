package net.researchgate.kafka.metamorph.kafka08.exceptions;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;

public class PartitionNotAvailableException extends PartitionConsumerException {

    private static final long serialVersionUID = 1L;

    public PartitionNotAvailableException() {
        super();
    }

    public PartitionNotAvailableException(String message) {
        super(message);
    }

    public PartitionNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartitionNotAvailableException(Throwable cause) {
        super(cause);
    }
}
