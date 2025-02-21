package com.singlestore.fivetran.destination.connector.warning_util;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WarningHandler<T> {
    StreamObserver<T> responseObserver;
    static final Logger logger = LoggerFactory.getLogger(WarningHandler.class);


    public WarningHandler(StreamObserver<T> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(String message) {
        logger.warn(message);
        responseObserver.onNext(createWarning(message));
    }

    public void handle(String message, Throwable t) {
        logger.warn(message, t);
        responseObserver.onNext(createWarning(String.format("%s: %s", message, t.getMessage())));
    }

    public abstract T createWarning(String message);
}
