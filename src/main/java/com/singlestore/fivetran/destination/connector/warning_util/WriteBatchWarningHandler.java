package com.singlestore.fivetran.destination.connector.warning_util;

import fivetran_sdk.v2.Warning;
import fivetran_sdk.v2.WriteBatchResponse;
import io.grpc.stub.StreamObserver;

public class WriteBatchWarningHandler extends WarningHandler<WriteBatchResponse> {

    public WriteBatchWarningHandler(StreamObserver<WriteBatchResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public WriteBatchResponse createWarning(String message) {
        return WriteBatchResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build()).build();
    }
}
