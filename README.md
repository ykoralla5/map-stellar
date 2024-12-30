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
Stellar-core: v22
Python 3.11.6
Kafka/Zookeeper 3.8.0
Debezium: 3.0.4
JDBC 3.0.4
Kafka-python-2.0.2

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
sudo curl
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
You can now run the node using `stellar-horizon` and monitor on curl http://localhost:8000/

Stellar horizon allows parallel ingestion operations for historical data while ingesting live data:
`stellar-horizon db reingest range <start> <end>`

### PostgreSQL database for filtered database of Stellar node
Since we only required ledger and transaction data, we ...
```
create database captivecore_filtered;
CREATE TABLE ledgers (column1 type1, column2 type2, column3 type3...);
CREATE TABLE transactions (column1 type1, column2 type2, column3 type3...);
		alter table ledgers add primary key (ledger_sequence); #unsure if primary or unique key
		alter table transactions add constraint fk_trans_led_seq foreign key (ledger_sequence) references ledgers (ledger_sequence);
		alter table transactions add primary key (transaction_hash); #unsure if primary or unique key

```
Tables, structure, primary keys
Roles and privileges for debezium and jdbc
Sample schema
Psql config changes like wal_level, etc
### Apache kafka
Connectors installation guide
Number of Brokers, partitions calculations
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

