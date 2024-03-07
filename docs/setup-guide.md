---
name: Setup Guide
title: SingleStore Setup Guide
description: Follow this guide to set up SingleStore as a destination in Fivetran.
---

# SingleStore Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

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
- A Fivetran account with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#rbacpermissions) permissions.


---

## Setup Instructions

### <span class="step-item"> Configure SingleStore </span>

1. Configure your firewall and/or other access control systems to allow incoming connections to your SingleStore instance from [Fivetran's IPs](https://fivetran.com/docs/using-fivetran/ips) for your region.
2. Ensure that the SingleStore database user has the following permissions:
    * `SELECT`
    * `INSERT`
    * `UPDATE`
    * `DELETE`
    * `CREATE`
    * `ALTER`
    * `CREATE DATABASE`

### <span class="step-item">Complete Fivetran configuration</span>

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), and then click **+ Add Destination**.
3. Enter a **Destination name** for your destination, and then click **Add**.
4. Select **SingleStore** as the destination type.
5. Enter the following connection configurations for you SingleStore workspace/cluster:
    * **Host**
    * **Port**
    * **Username**
    * **Password**
6. (Optional) Enable SSL and specify related configurations.
7. (Optional) Specify additional **Driver Parameters**. Refer to [The SingleStore JDBC Driver](https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters) documentation for a list of supported parameters.
8. Select the **Data processing location**.
9. Select your **Time zone**.
10. Click **Save & Test**.

Fivetran [tests and validates](/docs/destinations/singlestore/setup-guide#setuptest) the SingleStore connection configuration. Once the connection configuration test is successful, you can sync your data using Fivetran connectors to the SingleStore destination.

### Setup Test

Fivetran performs the following SingleStore connection test:

- The Connection test checks if Fivetran can connect to your SingleStore cluster using credentials provided in the setup form.
