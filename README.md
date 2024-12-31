# map-stellar
UZH Master Project Stellar

## Title
Data Pipeline for Stellar Blockchain using Apache Kafka and exploratory analysis of Stellar blockchain

## Description
As a part of our Masters project at the University of Zurich, we have designed a data pipeline which listens to a Stellar node and puts necessary data into a PostgreSQL database. With the collected data, we have conducted exploratory analysis of the blockchain such as inter-block finality time, number of ledgers over time, number of operations over time, quorum structure at a certain point in time, etc.

## Goal
The data pipeline provides data to the Stellar simulator.

## System architecture
Stellar captive core: Captive core is a mode of Stellar where the node doesn't participate in consensus. Stellar consists of 2 parts: Stellar-core and stellar-horizon. a traditional validating node would have to run both parts separately where Stellar-core contains data on the network and Stellar-horizon contains historical data. Since running a validator node was not of interest, we simply ran a captive core node where stellar core is automatically switched on and off when new ledgers are created.

PostgreSQL: Stellar uses PostgreSQL as the database to store blockchain data. For our second database where we store necessary data, we have also chosen PostgreSQL.

Apache Kafka: Apache Kafka will be used for event-streaming, i.e., reading the necessary data into our second database. We use connectors Debezium (source) and JDBC (sink) which read and write data, respectively to databases. Data capture is done using Write-Ahead Logs (WALs) of PostgreSQL.

## Pre-requisites
Software (with versions): postgresql, stellar captivecore, apache kafka
- Stellar-core: v22
- Python 3.11.6
- Kafka/Zookeeper 3.8.0
- Debezium: 3.0.4
- JDBC 3.0.4
- Kafka-python-2.0.2

## System requirements: 
Disk space: Our server had disk space of 3 TB and contained data from ledger sequences -- to --. We used various mechanisms to reduce disk usage (data compression lz4, kafka retention limits, etc).
RAM recommendations: Stellar recommends a RAM of 32 GB.
CPU recommendations: Stellar recommends a CPU of 4vCPU.

## Installation guide:
### Stellar captive core
#### Stellar horizon and stellar core package installation
config files: stellar-core.cfg
```
mkdir stellar-captive-core
cd stellar-captive-core
mkdir captivecoretablespace
```
Refer to [link](https://github.com/stellar/packages/blob/master/docs/adding-the-sdf-stable-repository-to-your-system.md#adding-the-sdf-stable-repository-to-your-system) to add Stellar Development Foundation's stable repository to your system.

Since our Ubuntu version was noble and stellar packages for horizon and core were only present on jammy, we added jammy to our apt sources:
```
echo "deb http://archive.ubuntu.com/ubuntu jammy main universe" | sudo tee /etc/apt/sources.list.d/jammy.list
sudo nano /etc/apt/sources.list.d/SDF.list
sudo apt update
```
Install stellar horizon and core packages:
```
sudo apt update
sudo apt install stellar-horizon stellar-core
```


### PostgreSQL database for Stellar node
Before running Stellar Captive core, a new empty database needs to be initialized. All historical data of the Stellar network is stored here. Below are the commands for PostgreSQL using `psql`
Create a new user for the database (you can set a password if required):
```
CREATE ROLE captivecore WITH LOGIN;
```
Initialize the database for stellar captive core and grant privileges on database and schema to created user:
```
CREATE DATABASE captivecoredb OWNER captivecore;
GRANT CREATE ON SCHEMA public TO captivecore;
GRANT USAGE ON SCHEMA public TO captivecore;
```

Once role and database definition are done, quit `psql`. Export the database details as an environment variable and install the schema onto the created database:
```
$ export DATABASE_URL= export DATABASE_URL=postgres://captivecore:password@localhost:5432/captivecoredb >> ~/.bashrc
$ stellar-horizon db init
```
Provide [further configurations](https://developers.stellar.org/docs/data/horizon/admin-guide/configuring) as environment variables. We ran horizon as a Single Instance Deployment with the below configurations:
```
export NETWORK=pubnet 
export STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
export STELLAR_CORE_CONFIG_PATH=/path/to/stellar-captive-core/stellar-core.cfg
export PARALLEL_JOB_SIZE=10
export MAX_DB_CONNECTIONS=80
```

### PostgreSQL database for filtered database of Stellar node
Since we only required ledger and transaction data, we ...
```
create database captivecore_filtered;
CREATE TABLE ledgers (column1 type1, column2 type2, column3 type3...);
CREATE TABLE transactions (column1 type1, column2 type2, column3 type3...);
ALTER TABLE transactions ADD CONSTRAINT transactions_id_key unique (id);
ALTER TABLE ledgers ADD CONSTRAINT ledgers_id_key unique (id);
```

### User creation for Kafka
It is recommended to create separate users for kafka and only give them necessary privileges. Since we use 2 databases, we created 2 users, one to read from our `captivecoredb` and one to write to `captivecoredb_fil`.

#### User debezium
This user will be used by the Kafka Source Connector Debezium to read any changes on the ledgers and transactions tables of the Stellar node. Create user and provide required privileges as per [Kafka Debezium requirements](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-permissions). The minimum privileges required are REPLICATION and LOGIN. WHY?
```
CREATE ROLE debezium;
ALTER ROLE debezium REPLICATION LOGIN;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT CONNECT ON DATABASE captivecoredb TO debezium;
```
Since user `debezium` needs to read from the ledgers and transactions tables of the Stellar captive core database, we give it privileges only on these tables:
```
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE history_transactions TO debezium;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE history_ledgers TO debezium;
```
Debezium uses the publicatons feature of PostgreSQL to note change events on the tables. Hence, we create a publication on the ledgers and transactions tables:
```
CREATE PUBLICATION captivecore_publication FOR TABLE public.history_ledgers, public.history_transactions;
GRANT CREATE ON DATABASE captivecoredb TO debezium;
```
PROVIDE TEXT HERE:
```
ALTER TABLE history_transactions REPLICA IDENTITY FULL; (since no unique column that has not null constraint)
ALTER TABLE history_ledgers REPLICA IDENTITY USING INDEX index_history_ledgers_on_sequence;
```
#### User JDBC
This user will be used by the Kafka Sink Connector JDBC to write any changes on the ledgers and transactions tables of the Stellar node onto the filtered database.

Create role and grant privileges to make operations insertion, updation and deletion on the ledgers and transactions tables on the filtered database.

```
CREATE ROLE jdbc;
GRANT CONNECT ON DATABASE captivecoredb_fil TO jdbc;
GRANT USAGE ON SCHEMA public TO jdbc;
GRANT INSERT, UPDATE, DELETE ON TABLE public.ledgers, public.transactions TO jdbc;
ALTER TABLE ledgers OWNER TO jdbc;
ALTER TABLE transactions OWNER TO jdbc;
```

Kafka's Debezium relies on WALs (Write-Ahead Logs) to capture any operations on a database. This requires the following PostgreSQL configuration changes (requires psql restart):
```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Additional configuration changes such as `max_wal_size` can be made to contain the size of the pg_wal folder. We also used `wal_compression = lz4` to further compress WAL files.

### Apache kafka
Kafka is an event streaming platform, i.e., it helps capture real-time data from sources such as databases (production of data) and allows further processing, storing of this real-time data (consumption of data). Events of production of data are read by Kafka `producers`, sent to their corresponding `topic` from where consumption is done by `consumers`. In our architecture, we will use Apache Kafka to read any incoming data into the Stellar node and filter and store the required data in a filtered database. The databases and the roles for Apache Kafka have already been created above.

Terms explanation:
Zookeeper: the main Kafka server which maintains ....
Kafka Connect: the part of Kafka that maintains connectors, topics, producers and consumers.

Debezium: Debezium is a connector which captures changes in databases. It has connectors for various databases such as MySQL, MariaDB, MongoDB, PostgreSQL, etc. 
Debezium JDBC: Debezium JDBC is a sink connector compatible with the Debezium source connector. It consumes data produced by the source connectors.
Due to limited RAM and the possible overhead that multiple partitions and brokers can cost, we kept a single partition and single broker.

#### Installation
Kafka: Kafka downloads are availabe [here](https://kafka.apache.org/downloads).
```
wget https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
```
Create a virtual environment inside the kafka directory to install required packages: `kafka-python, psycopg2`.

Connectors: Install debezium and JDBC inside a separate folder in the kafka directory (ex /kafka/connect). Ensure that Debezium and JDBC are of the same version.
[Debezium](https://debezium.io/documentation/reference/stable/install.html):
```
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.0.4.Final/debezium-connector-postgres-3.0.4.Final-plugin.tar.gz
tar -xvzf debezium-connector-postgres-3.0.4.Final-plugin.tar.gz
```

[JDBC](https://debezium.io/documentation/reference/stable/connectors/jdbc.html#jdbc-deployment):
```
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/3.0.4.Final/debezium-connector-jdbc-3.0.4.Final-plugin.tar.gz
tar -xvzf debezium-connector-jdbc-3.0.4.Final-plugin.tar.gz
```

Kafka property changes:
Add the path of the folder containing the extracted Debezium and JDBC connectors as plugin.path in the Kafka Connect properties (/kafka/config/connect-distributed.properties)

#### Configuration
Kafka Connect: Kafka has multiple configuration options such as multiple Kafka servers (also called brokers), each topic having multiple partitions and different consumers being allotted to certain topic partitions.

Create a directory (/kafka/connectors-configs) to store the JSON files containing the configurations for the connectors.

Debezium: The configuration file for debezium is present at ---. You can whitelist the tables and the specific columns that the connector can listen to (ensure that the database role for debezium has access to these tables and columns). The connector listens to publication previously created to capture any new changes on the Stellar database. 

JDBC: The configuration file for debezium is present at ---. We created separate connectors to write to ledgers and transactions tables of the filtered database. It is important to provide a uniquely identifiable column to the connector to uniquely identify the records. We used `record_key` which are the unique keys created by Kafka during event creation. We set `schema.evolution`, which allows the destination database schema to adjust to the structure of the incoming event, in case the structure of tables in the Stellar database change in the future.

### Running
Before running the stellar node, start Zookeeper, Kafka server and Kafka Connect:
```
/path/to/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties #start zookeeper service
/path/to/kafka/bin/kafka-server-start.sh config/server.properties #start kafka broker service in new terminal
/path/to/kafka/bin/connect-distributed.sh config/connect-distributed.properties or config/connect-standalone.properties
```

Make a POST command to send the connector configurations:
```
curl -X POST -H "Content-Type: application/json" --data @debezium-postgres-connector.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @captivecore-ledgers-sink-connector.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @captivecore-transactions-sink-connector.json http://localhost:8083/connectors
```

Get connector status using:
```
curl -s http://localhost:8083/connectors/<connector-name>/status | jq
```

Everytime a change is made on the connector configuration, the connector needs to be updated using a PUT request.
```
curl -X PUT -H "Content-Type: application/json" --data @<connector-config-file>.json http://localhost:8083/connectors/<connector-name>/config
```
You can also delete connectors using:
```
curl -X DELETE http://localhost:8083/connectors/<connector-name>
```
You can now run the Stellar node using `stellar-horizon` and monitor on http://localhost:11626/info

Stellar horizon allows parallel ingestion operations for historical data while ingesting live data:
`stellar-horizon db reingest range <start> <end>`. Note that if `HISTORY_RETENTION_COUNT` is provided in the stellar core configuration, any extra ledgers beyond this count are deleted. Also note that if the parallel ingestion ledgers overlap with the existing ledgers in the database, they are **overwritten**.

### Monitoring
Monitor sync status of the Stellar node using: `curl http://localhost:11626/info`. Note that this does not show the status of any parallel reingestion operations. This can be monitored by querying the database.

Process of downloading ledger: Downloading and verifying buckets -> Applying buckets -> Download and apply checkpoints

Monitor lag of consumed events using:
`SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS replication_lag_size FROM pg_replication_slots;`



Example commands
Kafka connect with WALs:
Connect-distributed.properties
Code for consumer:
Compare ledger and transaction ciunt between original db and second db
## Exploratory analysis
## Testing and troubleshooting:
### Common errors
Disk usage issues like kafka logs (reduce 7 day retention time)
Postgresql login issues (peer authentication failed, grant rights to user)

Common errors found, possible reasons and how to solve
## Useful commands
Stellar:
/info
/metrics
Kafka:
List of connectors
List of topics
## Additional files:
### Example kafka messages
### Diagrams

