# SingleStoreDB Fivetran Destination

## Pre-requisites for development
- JDK v17
- Gradle 8 ([here](https://gradle.org/install/#manually) is an installation instruction)

## Steps for starting server
1. Build the Jar
```
gradle jar
```
2. Run the Jar
```
java -jar build/libs/singlestoredb-fivetran-destination.jar 
```

## Steps for running Java tests
1. Start SingleStoreDB cluster
```
docker run \
    -d --name singlestoredb-dev \
    -e SINGLESTORE_LICENSE="YOUR SINGLESTORE LICENSE" \
    -e ROOT_PASSWORD="YOUR SINGLESTORE ROOT PASSWORD" \
    -p 3306:3306 -p 8080:8080 -p 9000:9000 \
    ghcr.io/singlestore-labs/singlestoredb-dev:latest
```
2. Create `ROOT_PASSWORD` environment variable
```
export ROOT_PASSWORD="YOUR SINGLESTORE ROOT PASSWORD"
```
3. Run tests
```
gradle build
```

## Steps for using Destination tester
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
