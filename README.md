# SingleStore Fivetran Destination Connector

## Pre-requisites for development

- JDK v17
- Gradle 8 ([here](https://gradle.org/install/#manually) is an installation instruction)

## Steps for starting server

1. Download proto files

```
wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/common.proto
wget -O src/main/proto/destination_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/destination_sdk.proto
```

2. Build the Jar

```
gradle jar
```

2. Run the Jar

```
java -jar build/libs/singlestore-fivetran-destination-connector-2.0.8.jar
```

## Steps for running Java tests

1. Start SingleStore cluster

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

3. Download proto files

```
wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/common.proto
wget -O src/main/proto/destination_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/destination_sdk.proto
```

4. Run tests

```
gradle build
```

## Steps for using Destination tester

1. Start SingleStore cluster
2. Re-create `tester` database

```
drop database if exists tester;
create database tester;
```

3. To run the tester follow instructions
   from [here](https://github.com/fivetran/fivetran_sdk/blob/v2/tools/destination-connector-tester/README.md). As a
   command use You can use

```
docker run --mount type=bind,source=<PATH TO PROJECT>/data-folder,target=/data -a STDIN -a STDOUT -a STDERR -it -e WORKING_DIR=./data-folder -e GRPC_HOSTNAME=localhost --network=host us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<tag> --tester-type destination --port 50052
```
