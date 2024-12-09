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
Stellar-core: v21.2.0-13-ga6d6f08fb
Python 3.11.6
Kafka/Zookeeper 3.8.0
Debezium: 3.0.0
JDBC 2.7.3
Kafka-python-2.0.2

## System requirements: 
Disk space: Our server had disk space of 3 TB and contained data from ledger sequences -- to --. We used various mechanisms to reduce disk usage (data compression, kafka retention limits, etc)
RAM recommendations

## Installation guide:
### Stellar captive core
config files: stellar-core.cfg
Stellar horizon and stellar core installation
DB roles
Environment variables
Link to stellar’s docs for more configuration
### Psql: for db2
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

