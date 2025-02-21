---
name: SingleStore
title: SingleStore Destination connector by Fivetran | Fivetran documentation
description: Connect your data sources to SingleStore using Fivetran in just minutes. Explore our documentation and start syncing your applications, databases, and events.
hidden: false
---

# SingleStore {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}


[SingleStore](https://www.singlestore.com/) is a distributed, cloud-native database that can handle transactional and analytical workloads with a unified engine. It provides real-time analytics, transactions, and streaming capabilities, enabling users to handle diverse workloads on a single platform. 

You can use Fivetran to ingest data from various sources into SingleStore for unified analytics and insights. 

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to SingleStore Destination connector and its documentation, contact SingleStore by raising an issue in the [SingleStore Fivetran Destination Connector](https://github.com/singlestore-labs/singlestore-fivetran-destination) GitHub repository.

----

## Setup Guide

Follow our step-by-step [SingleStore Setup Guide](/docs/destinations/singlestore/setup-guide) to connect SingleStore to Fivetran.

----

## Type Transformation Mapping

While extracting data from your data source using Fivetran, SingleStore matches Fivetran data types to SingleStore data types. If a data type isn't supported, it is automatically typecast to the closest supported SingleStore data type.

The following table illustrates how Fivetran data types are transformed into SingleStore supported types:

| Fivetran Data Type | SingleStore Data Type      |
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

## Schema Changes

| Schema Change          | Supported | Notes                                                                                                                                                                                                                                                                                                                                   |
|------------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Add column                    | ✔       | When Fivetran detects the addition of a column in your source, it automatically adds that column in the SingleStore Destination connector.                                                                                                                                                                                              |
| Change column type            | ✔       | When Fivetran detects a change in the column type in the data source, it automatically changes the column type in the SingleStore Destination connector. To change the column type, Fivetran creates a new column, copies the data from the existing column to the new column, deletes the existing column, and renames the new column. |
| Change key or key column type | ✔       | Changing PRIMARY KEY is not supported in SingleStore. When Fivetran detects a change in a key, it creates a new table with updated PRIMARY KEY, copies the data from the existing table to the new one, deletes the existing table, and renames the new table                                                                           |