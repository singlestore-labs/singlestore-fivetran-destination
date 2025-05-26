---
name: Setup Guide
title: SingleStore Setup Guide
description: Follow this guide to set up SingleStore as a destination in Fivetran.
---

# SingleStore Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="singlestore" /%}

Follow the steps in this guide to connect SingleStore to Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to SingleStore destination and its documentation, contact SingleStore by raising an issue in the [SingleStore Fivetran Destination](https://github.com/singlestore-labs/singlestore-fivetran-destination) GitHub repository.

---

## Prerequisites


- A SingleStore instance. Refer to [Creating and Using Workspaces](https://docs.singlestore.com/cloud/getting-started-with-singlestore-helios/about-workspaces/creating-and-using-workspaces/) for instructions on creating a SingleStore workspace in the [Cloud Portal](https://portal.singlestore.com/).
  To deploy a Self-Managed cluster instead, refer to [Deploy](https://docs.singlestore.com/db/latest/deploy/). Once the SingleStore workspace/cluster is Active, you'll need the following to connect to Fivetran:
    - `Host`: Hostname or IP address of the SingleStore workspace/cluster
    - `Port`: Default is `3306`
    - `Username`: Username of the SingleStore database user
    - `Password`: Password of the SingleStore database user    
- A Fivetran account with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#rbacpermissions) permissions.


---

## Setup instructions

### <span class="step-item"> Configure SingleStore </span>

1. Configure your firewall and/or other access control systems to allow incoming connections to your SingleStore instance from [Fivetran's IPs](/docs/using-fivetran/ips) for your region.
2. Ensure that the SingleStore database user has the following permissions:
    * `SELECT`
    * `INSERT`
    * `UPDATE`
    * `DELETE`
    * `CREATE`
    * `ALTER`
    * `CREATE DATABASE` (if `database` configuration is not specified)

### <span class="step-item">Complete Fivetran configuration</span>

1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and click **Add destination**.
3. Enter a **Destination name** for your destination, and then click **Add**.
4. Select **SingleStore** as the destination type.
5. Enter the following connection configurations for you SingleStore workspace/cluster:
    * **Host**
    * **Port**
    * **Username**
    * **Password**
6. (Optional) Enter a **Database** configuration if you want all tables to be created in a single database.
7. (Optional) Enable SSL and specify related configurations.
8. (Optional) Specify additional **Driver Parameters**. Refer to [The SingleStore JDBC Driver](https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters) documentation for a list of supported parameters.
9. Select the **Data processing location**.
10. Select your **Time zone**.
11. Copy the [Fivetran's IP addresses (or CIDR)](/docs/using-fivetran/ips) that you _must_ safelist in your firewall.
11. Click **Save & Test**.

Fivetran [tests and validates](/docs/destinations/singlestore/setup-guide#setuptest) the SingleStore connection configuration. Once the connection configuration test is successful, you can sync your data using Fivetran connectors to the SingleStore destination.

In addition, Fivetran automatically configures a [Fivetran Platform Connector](/docs/logs/fivetran-platform) to transfer the connector logs and account metadata to a schema in this destination. The Fivetran Platform Connector enables you to monitor your connectors, track your usage, and audit changes. The connector sends all these details at the destination level.

> IMPORTANT: If you are an Account Administrator, you can manually add the Fivetran Platform Connector on an account level so that it syncs all the metadata and logs for all the destinations in your account to a single destination. If an account-level Fivetran Platform Connector is already configured in a destination in your Fivetran account, then we don't add destination-level Fivetran Platform Connectors to the new destinations you create.

### Setup test

Fivetran performs the following SingleStore connection test:

- The Connection test checks if Fivetran can connect to your SingleStore cluster using credentials provided in the setup form.
