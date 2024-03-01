---
name: Setup Guide
title: SingleStore Setup Guide
description: Follow the guide to set up SingleStore as a destination.
---

# SingleStore Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

Follow the setup guide to connect SingleStore to Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to SingleStore destination and its documentation, contact SingleStore by raising an issue in the connectors [GitHub repository](https://github.com/singlestore-labs/singlestore-fivetran-destination).

---

## Prerequisites

To connect SingleStore to Fivetran, you need the following:

- A SingleStore instance. To obtain it you can create SingleStore workspace in [Cloud Portal](https://portal.singlestore.com/?_gl=1*1uhqemo*_ga*NjMxODEwNTgzLjE3MDY1MzQwNDM.*_ga_V9YBY81TXW*MTcwOTI4OTkyNC4yNi4xLjE3MDkyOTA0MDEuNjAuMC4w) using instructions from [Creating and Using Workspaces
](https://docs.singlestore.com/cloud/getting-started-with-singlestore-helios/about-workspaces/creating-and-using-workspaces/) or you can deploy a Self-Managed cluster in various way using instructions from [Deploy](https://docs.singlestore.com/db/latest/deploy/). After you spin up SingleStore, you should have:
    - `Host`
    - `Port`
    - `Username`
    - `Password`    
- A Fivetran account with [permission to add destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#legacyandnewrbacmodel).


---

## Setup instructions

### <span class="step-item"> Configure SingleStore </span>

1. Configure your firewall and/or other access control systems to allow incoming connections to your SingleStore instance from [Fivetran's IPs](https://fivetran.com/docs/using-fivetran/ips) for your region.
2. Ensure that your SingleStore user has following permissions:
    * `SELECT`
    * `INSERT`
    * `UPDATE`
    * `DELETE`
    * `CREATE`
    * `ALTER`
    * `CREATE DATABASE`

### <span class="step-item">Finish Fivetran configuration </span>

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), and then click **+ Add Destination**.
3. Enter a **Destination name** of your choice.
4. Click **Add**.
5. Select **SingleStore** as the destination type.
6. Enter the **Host**
7. Enter the **Port** number.
8. Enter the **Username**
9. Enter the **Password**
10. (Optinal) Enable SSL and specify related configurations.
11. (Optinal) Specify aditional **Driver Parameters**. Full list of supported parameters can be found [here](https://docs.singlestore.com/db/v8.5/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters) 
12. Choose the **Data processing location**.
13. Choose your **Time zone**.
14. Click **Save & Test**.

Fivetran [tests and validates](/docs/destinations/singlestore/setup-guide#setuptest) the SingleStore connection. On successful completion of the setup test, you can sync your data using Fivetran connectors to the SingleStore destination.

### Setup test

Fivetran performs the following SingleStore connection test:

- The Connection test checks if we can connect to your SingleStore cluster using credentials provided in the setup form.
