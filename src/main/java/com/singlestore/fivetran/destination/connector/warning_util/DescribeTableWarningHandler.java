package com.singlestore.fivetran.destination.connector.warning_util;


import fivetran_sdk.v2.DescribeTableResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

public class DescribeTableWarningHandler extends WarningHandler<DescribeTableResponse> {

    public DescribeTableWarningHandler(StreamObserver<DescribeTableResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public DescribeTableResponse createWarning(String message) {
        return DescribeTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build()).build();
    }
}
