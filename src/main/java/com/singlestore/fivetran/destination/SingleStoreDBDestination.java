package com.singlestore.fivetran.destination;

import io.grpc.*;
import org.apache.commons.cli.*;

import java.io.IOException;

public class SingleStoreDBDestination {
    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        Options options = new Options();
        Option portOption = new Option("p", "port", true, "port which server will listen");
        options.addOption(portOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            Logger.severe("Failed to parse arguments", e);
            formatter.printHelp("singlestoredb-fivetran-destination", options);

            throw e;
        }
        
        String portStr = cmd.getOptionValue("port", "50052");
        int port = 50052;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            Logger.warning("Failed to parse --port option", e);
            formatter.printHelp("singlestoredb-fivetran-destination", options);

            throw e;
        }

        Logger.info(String.format("Starting Destination gRPC server which listens port %d", port));
        Server server = ServerBuilder
                .forPort(port)
                .addService(new SingleStoreDBDestinationServiceImpl()).build();

        server.start();
        Logger.info(String.format("Destination gRPC server started"));
        server.awaitTermination();
    }
}
