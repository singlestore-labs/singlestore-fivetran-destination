---
name: SingleStore
title: Fivetran for SingleStore | Configuration and Documentation
description: Connect your data sources to SingleStore using Fivetran in just minutes. Explore our documentation and start syncing your applications, databases, and events.
menuPosition: 170
---

# SingleStore {% badge text="Partner-Built" /%} {% availabilityBadge connector="singlestore" /%}


[SingleStore](https://www.singlestore.com/) is a distributed, cloud-native database that can handle transactional and analytical workloads with a unified engine. It provides real-time analytics, transactions, and streaming capabilities, enabling users to handle diverse workloads on a single platform. 

You can use Fivetran to ingest data from various sources into SingleStore for unified analytics and insights. 

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to SingleStore destination and its documentation, contact SingleStore by raising an issue in the [SingleStore Fivetran Destination](https://github.com/singlestore-labs/singlestore-fivetran-destination) GitHub repository.

----

{% partial file="destinations/saas-supported-deployment-models.template.md" /%}

-----

## Setup guide

Follow our step-by-step [SingleStore Setup Guide](/docs/destinations/singlestore/setup-guide) to connect SingleStore to Fivetran.

----

## Type transformation mapping

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

## Schema changes

| Schema Change          | Supported | Notes                                                                                                     |
|------------------------|-----------|-----------------------------------------------------------------------------------------------------------|
| Add column             | Yes       | When Fivetran detects the addition of a column in your source, it automatically adds that column in the SingleStore destination. |
| Change column type     | Yes       | When Fivetran detects a change in the column type in the data source, it automatically changes the column type in the SingleStore destination. To change the column type, Fivetran creates a new column, copies the data from the existing column to the new column, deletes the existing column, and renames the new column. |
| Change key             | No        | Changing PRIMARY KEY column is not supported in SingleStore. |
| Change key column type | No        | Changing PRIMARY KEY column data type is not supported in SingleStore. |

----------

## Limitations

Fivetran does not support [history mode](/docs/core-concepts/sync-modes/history-mode) for SingleStore destinations.
