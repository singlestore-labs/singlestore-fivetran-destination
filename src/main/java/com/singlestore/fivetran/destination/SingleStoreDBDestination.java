package com.singlestore.fivetran.destination;

import io.grpc.*;

import java.io.IOException;

public class SingleStoreDBDestination {
    public static void main(String[] args) throws InterruptedException, IOException {
        // TODO: PLAT-6893 parse port from args
        int port = 50052;

        Server server = ServerBuilder
                .forPort(port)
                .addService(new SingleStoreDBDestinationServiceImpl()).build();

        server.start();
        // TODO: PLAT-6892 make consistent logging
        System.out.println("Destination gRPC server started");
        server.awaitTermination();
    }
}
