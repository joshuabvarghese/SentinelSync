# 🔄 SentinelSync

**"The Nervous System of the Infrastructure"**

A fault-tolerant data replication bridge that ensures consistency between relational and non-relational data stores at scale.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Overview

SentinelSync implements the **Change Data Capture (CDC)** pattern to stream database updates from PostgreSQL through Apache Kafka into Apache Cassandra, providing:

- **Decoupled Architecture**: Kafka acts as a high-throughput buffer preventing data loss during traffic spikes
- **Heterogeneous Replication**: Translates relational SQL rows into wide-column NoSQL schema
- **At-Least-Once Delivery**: Implements Kafka offset management for crash recovery
- **Idempotent Writes**: Engineered Cassandra insertion logic prevents data duplication

## 🏗️ Architecture

```
┌─────────────┐     CDC Events      ┌─────────────┐     Consume      ┌─────────────┐
│             │ ──────────────────> │             │ ──────────────> │             │
│ PostgreSQL  │                     │    Kafka    │                 │  Cassandra  │
│  (Source)   │  Logical Replication│  (Buffer)   │  Offset Mgmt    │   (Sink)    │
│             │ <──────────────────│             │ <──────────────│             │
└─────────────┘     Heartbeat       └─────────────┘    Ack/Retry    └─────────────┘
```

## 📦 Tech Stack

| Component       | Technology                    | Purpose                          |
|-----------------|-------------------------------|----------------------------------|
| **Source DB**   | PostgreSQL 15                 | Relational data source           |
| **Message Bus** | Apache Kafka 3.6              | Event streaming platform         |
| **Sink DB**     | Apache Cassandra 4.1          | Distributed NoSQL storage        |
| **CDC Engine**  | Python 3.11                   | Custom replication service       |
| **Libraries**   | confluent-kafka, cassandra-driver | Kafka/Cassandra clients      |
| **Serialization** | JSON/Avro                   | Message format                   |
## 🗂️ Project Structure

```
sentinelsync/
├── src/
│   ├── cdc/
│   │   ├── __init__.py
│   │   ├── postgres_cdc.py      # PostgreSQL change capture
│   │   ├── kafka_producer.py    # Kafka event publisher
│   │   ├── kafka_consumer.py    # Kafka event consumer
│   │   └── cassandra_writer.py  # Idempotent sink writer
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py          # Configuration management
│   └── main.py                  # Service entrypoint
├── tests/
│   ├── test_cdc.py
│   ├── test_kafka.py
│   └── test_cassandra.py
├── docker/
│   ├── docker-compose.yml       # Full stack orchestration
│   └── init-scripts/
│       ├── init-postgres.sql
│       └── init-cassandra.cql
├── config/
│   └── app.yaml                 # Application configuration
├── requirements.txt
├── Makefile
└── README.md
```
