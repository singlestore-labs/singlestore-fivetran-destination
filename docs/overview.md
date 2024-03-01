---
name: SingleStore
title: Fivetran for SingleStore | Configuration and documentation
description: Connect data sources to SingleStore in minutes using Fivetran. Explore documentation and start syncing your applications, databases, and events.
---

# SingleStore {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}


[SingleStore](https://www.singlestore.com/) is a distributed database. It provides real-time analytics, transactions, and streaming capabilities, enabling users to handle diverse workloads on a single platform. 

You can use Fivetran, to ingest data from various sources into SingleStore for unified analytics and insights. 

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to SingleStore destination and its documentation, contact SingleStore by raising an issue in the connectors [GitHub repository](https://github.com/singlestore-labs/singlestore-fivetran-destination).

----

## Setup guide

Follow our [step-by-step SingoeStore setup guide](/docs/destinations/singlestore/setup-guide) to connect SingleStore to Fivetran.

----

## Type transformation mapping

As we extract your data, we match Fivetran data types to types that SingleStore supports. If we don't support a specific data type, we automatically change that type to the closest supported data type.

The following table illustrates how we transform your Fivetran data types into SingleStore-supported types:

| FIVETRAN DATA TYPE | SingleStore DATA TYPE      |
|--------------------|----------------------------|
| BOOLEAN            | BOOLEAN                    |
| SHORT              | SMALLINT                   |
| INT                | INT                        |
| LONG               | BIGINT                     |
| BIGDECIMAL         | DECIMAL                    |
| FLOAT              | FLOAT                      |
| DOUBLE             | DOUBLE                     |
| LOCALDATE          | DATE                       |
| LOCALDATETIME      | DATETIME(6)                |
| INSTANT            | DATETIME(6)                |
| STRING             | TEXT                       |
| XML                | TEXT                       |
| JSON               | JSON                       |
| BINARY             | BLOB                       |

----

## Schema changes

| SCHEMA CHANGE      | SUPPORTED | NOTES                                                                                                     |
|--------------------|-----------|-----------------------------------------------------------------------------------------------------------|
| Add column         | Yes       | When Fivetran detects a column being added to your source, Fivetran automatically adds that column in SingleStore. |
| Change column type | Yes       | When Fivetran detects a column type change, Fivetran automatically changes column in SingleStore. To perform it, fake column is created, data is copied, old column is deleted and a new one is renamed. |
---
