package net.researchgate.kafka.metamorph.kafka08.exceptions;

import net.researchgate.kafka.metamorph.exceptions.PartitionConsumerException;

public class OffsetFetchException extends PartitionConsumerException {

    private static final long serialVersionUID = 1L;

    public OffsetFetchException() {
        super();
    }

    public OffsetFetchException(String message) {
        super(message);
    }

    public OffsetFetchException(String message, Throwable cause) {
        super(message, cause);
    }

    public OffsetFetchException(Throwable cause) {
        super(cause);
    }
}
