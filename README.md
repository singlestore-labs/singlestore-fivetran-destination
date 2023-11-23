# SingleStoreDB Fivetran Destination

## Pre-requisites for development
- JDK v17
- Gradle 8 ([here](https://gradle.org/install/#manually) is an installation instruction)

## Steps for starting server
1. Build the Jar
```
> gradle jar
```
2. Run the Jar
```
> java -jar build/libs/singlestoredb-fivetran-destination.jar 
```

## Steps for testing
1. Start SingleStoreDB cluster
2. Re-create `tester` database
```
drop database if exists tester;
create database tester;
```
3. To run the tester follow instructions from [here](https://github.com/fivetran/fivetran_sdk/blob/main/tools/destination-tester/README.md). As a command use You can use
```
docker run --mount type=bind,source=./data-folder,target=/data -a STDIN -a STDOUT -a STDERR -it -e WORKING_DIR=./data-folder -e GRPC_HOSTNAME=localhost --network=host sdk-destination-tester --plain-text
```
