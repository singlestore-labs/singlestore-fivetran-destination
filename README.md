# SingleStoreDB Fivetran Destination

## Pre-requisites for development
- JDK v17
- Gradle 8

## Steps
1. Build the Jar
```
> gradle jar
```
2. Run the Jar
```
> java -jar build/libs/JavaDestination.jar 
```
3. To run tester follow instructions from [here](https://github.com/fivetran/fivetran_sdk/blob/main/tools/destination-tester/README.md)