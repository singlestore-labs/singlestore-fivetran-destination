# SingleStoreDB Fivetran Destination

## Pre-requisites for development
- JDK v17
- Gradle 8 ([here](https://gradle.org/install/#manually) is an installation instruction)

## Steps
1. Build the Jar
```
> gradle jar
```
2. Run the Jar
```
> java -jar build/libs/singlestoredb-fivetran-destination.jar 
```
3. To run the tester follow instructions from [here](https://github.com/fivetran/fivetran_sdk/blob/main/tools/destination-tester/README.md). You can use `./data-folder` as `<local-data-folder>`.
