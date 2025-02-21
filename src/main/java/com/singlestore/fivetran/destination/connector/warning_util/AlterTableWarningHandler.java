package com.singlestore.fivetran.destination.connector.warning_util;

import fivetran_sdk.v2.AlterTableResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

public class AlterTableWarningHandler extends WarningHandler<AlterTableResponse> {

    public AlterTableWarningHandler(StreamObserver<AlterTableResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public AlterTableResponse createWarning(String message) {
        return AlterTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build()).build();
    }
}
