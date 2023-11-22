package com.singlestore.fivetran.destination;

import io.grpc.*;
import org.apache.commons.cli.*;

import java.io.IOException;

public class SingleStoreDBDestination {
    public static void main(String[] args) throws InterruptedException, IOException {
        Options options = new Options();
        Option portOption = new Option("p", "port", true, "port which server will listen");
        options.addOption(portOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            // TODO: PLAT-6892 make consistent logging
            System.out.println(e.getMessage());
            formatter.printHelp("singlestoredb-fivetran-destination", options);

            System.exit(1);
        }
        
        String portStr = cmd.getOptionValue("port", "50052");
        int port = 50052;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            // TODO: PLAT-6892 make consistent logging
            System.out.printf("Failed to parse --port option: %s%n", e.getMessage());
        }

        // TODO: PLAT-6892 make consistent logging
        System.out.printf("Starting Destination gRPC server which listens port %d%n", port);
        Server server = ServerBuilder
                .forPort(port)
                .addService(new SingleStoreDBDestinationServiceImpl()).build();

        server.start();
        // TODO: PLAT-6892 make consistent logging
        System.out.println("Destination gRPC server started");
        server.awaitTermination();
    }
}
