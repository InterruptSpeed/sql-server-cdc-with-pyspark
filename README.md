# sql-server-cdc-with-pyspark
this repo contains an example of how to get pyspark and delta tables working with sql server change data capture

## Prerequisites

* SQL Server w/ login credentials and authorization to create objects and adjust settings
* pyspark:
```
$ virtualenv ./venv
$ ./venv/bin/activate
$ pip install -r requirements.txt
```

## Scenario

* We need:
  * to get data from our line of business application's SQL Server backend
  * to use that data for analytical purposes
  * the data to be available soon after it is changed in the application
  * to get only what has changed; not all of the data every time.

* We don't have:
  * a Kafka-style message broker
  * a Debezium appliance to shunt SQL Changes to the broker even if we had one
  * capability to use structured streaming in the typical pyspark way [see above]

## Solution

* We use a combination of:
  * SQL Server Change Data Capture capabilities
  * pyspark to create the initial delta and hive tables
  * pyspark to "continuously" read from the CDC tables and merge changes into delta where "continuously" means frequent micro-batches.

[![solution design diagram](design.png?123)](design.png?123)
## Tutorial

* The backend of our line of business application is found in [app_db.sql](app_db.sql). Start by executing:
```
  -- create the customers table
CREATE TABLE dbo.customers (
    customer_id INT NOT NULL IDENTITY PRIMARY KEY,
    first_name  NVARCHAR(128) NOT NULL,
    last_name   NVARCHAR(128) NOT NULL,
    email       NVARCHAR(128) NOT NULL,
    city        NVARCHAR(128) NOT NULL
);

-- insert a record into it
INSERT INTO dbo.customers VALUES(
    'Bruce',
    'Wayne',
    'bwayne@example.com',
    'Gotham City'
)
```
* Load up the [init_env.ipynb](init_env.ipynb) notebook and update your credentials. Be sure that your IP is in the firewall for the SQL Server you are targeting and shutdown the notebook when it is complete.
  * This notebook will connect to the SQL server and create a Hive table in your spark environment.
  * This Hive table is backed by a delta table which contains the initial data and schema from your database.
* Now we need to enable CDC on SQL Server and insert more records:
```
-- enable CDC on the database
EXEC sys.sp_cdc_enable_db;
-- enable CDC on the customers table
-- NOTE: will not indicate a change for existing data
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'customers',
  @role_name     = NULL;

-- insert more records
INSERT INTO dbo.customers VALUES(
    'Clark',
    'Kent',
    'ckent@example.com',
    'Metropolis'
);
INSERT INTO dbo.customers VALUES(
    'Diana',
    'Prince',
    'dprince@example.com',
    'UNKNOWN'
);
```
* Load up the [cdc_process.ipynb](cdc_process.ipynb) notebook and update your credentials.
  * This notebook will connect to the SQL server and get the latest CDC data.
  * It will merge those changes into the delta table and the changes will be reflected in the Hive table.
  * Run this notebook as often as you like to recieve any changes. Consider adjusting the CDC retention period on SQL server to be greater than the rerun interval but not terribly so [e.g. retention period is 30 mins and notebooks runs every 5 mins]. This will minimize the amount of duplicate merging that happens.
* Now we will pretend Lex Luthor has broken into our application:
```
-- at some point Lex Luthor has broken in...
INSERT INTO dbo.customers VALUES(
    'Lex',
    'Luthor',
    'lluthor@haxxordu.com',
    'Everywhere!!!'
);

-- and then revealed Clark's secret email
UPDATE dbo.customers
SET email = 'superman@lexwashere.com'
WHERE customer_id = 2;
```
* And if the cdc_process notebook is run again those changes will be reflected as soon as the CDC scan registers them.
* Of course Cyborg is on the case and is fixing what Luthor has done:
```
-- cyborg is on the case and removes Lex's entry
DELETE FROM dbo.customers
WHERE email = 'lluthor@haxxordu.com';

-- and smoothes things over for Supes; nothing to see here
UPDATE dbo.customers
SET email = 'ckent@example.com', city = 'Smallville'
WHERE customer_id = 2;
```
* And once again run the cdc_process notebook to see all is well in the Watchtower.

## Notes

* the notebooks can also be exported as .py files and just run as normal pyspark jobs:
```
python init_env.py
```
AND
```
python cdc_process.py
```
* code isn't as DRY as it could be but I'm going for tutorial material
* delta_table_path could use ADLS just change as necessary
* mutiple tables could be processed in this same way like so:
```
for src_table, src_table_key in zip(src_tables, src_table_keys):
  ..
```
or you could have a fancy dataclass or tuple or whatever