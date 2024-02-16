package com.singlestore.fivetran.destination;

import io.grpc.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleStoreDestination {
    private static final Logger logger = LoggerFactory.getLogger(SingleStoreDestination.class);

    public static void main(String[] args)
            throws InterruptedException, IOException, ParseException {
        Options options = new Options();
        Option portOption = new Option("p", "port", true, "port which server will listen");
        options.addOption(portOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Failed to parse arguments", e);
            formatter.printHelp("singlestore-fivetran-destination", options);

            throw e;
        }

        String portStr = cmd.getOptionValue("port", "50052");
        int port = 50052;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse --port option", e);
            formatter.printHelp("singlestore-fivetran-destination", options);

            throw e;
        }

        logger.info(String.format("Starting Destination gRPC server which listens port %d", port));
        Server server = ServerBuilder.forPort(port)
                .addService(new SingleStoreDestinationServiceImpl()).build();

        server.start();
        logger.info(String.format("Destination gRPC server started"));
        server.awaitTermination();
    }
}
