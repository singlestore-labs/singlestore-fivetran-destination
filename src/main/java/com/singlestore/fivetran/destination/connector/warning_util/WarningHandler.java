package com.singlestore.fivetran.destination.connector.warning_util;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarningHandler {
    static final Logger logger = LoggerFactory.getLogger(WarningHandler.class);

    public void handle(String message) {
        logger.warn(message);
    }

    public void handle(String message, Throwable t) {
        logger.warn(message, t);
    }
}
