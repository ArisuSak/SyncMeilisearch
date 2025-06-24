# Synchronize Meilisearch in Go

This project syncs data between PostgreSQL and Meilisearch using PostgreSQL's Write-Ahead Logs (WAL) in Go.  
It provides an option to stream data changes using NATS JetStream for real-time synchronization.

## Prerequisites

- Go 1.16 or later
- PostgreSQL
- NATS Server (optional)
- Meilisearch

## Install Dependencies

```sh
go mod tidy
```

## PostgreSQL Configuration for WAL Replication

To enable data synchronization using PostgreSQL's Write-Ahead Logs (WAL), you need to adjust your PostgreSQL server settings.

### Update `postgresql.conf`:

- Set the WAL level to `logical` to allow logical replication:

Create a .env file based on the .env.example template and configure your environment variables.

```sh
wal_level = logical
```

- Increase the maximum number of replication slots:

```sh
max_replication_slots = 4
```

- Increase the maximum number of WAL senders:

```sh
max_wal_senders = 4
```

### Update `pg_hba.conf`:

Add a line to allow replication connections (adjust IP range as needed):

```sh
host replication all 0.0.0.0/0 md5
```

### Restart PostgreSQL:

After making these changes, restart your PostgreSQL server for the settings to take effect.

---

These configurations ensure PostgreSQL can stream WAL changes, which the application uses to sync data between PostgreSQL and Meilisearch via NATS JetStream.

---

## Important config variables:

### config.ymal

```sh
initialize:
  true
  # If true, the script will create the necessary tables and indexes in the database.
  # If false, it will assume the tables and indexes already exist.

meilisearch:
  api_url: http://localhost:7700
  api_key: "idk"
database:
  type: postgres
  host: localhost
  port: 5432
  database: mydb
  user: myuser
  password: mypassword
sync:
  - table: table_1_name
    index: index_name
    pk: primary_key_name
  - table: table_2_name
    index: index_name
    pk: primary_key_name
  - table: table_3_name
    index: index_name
    pk: primary_key_name
```

### .env

To enable integration with a streaming service like NATS JetStream, configure the following environment variables in your .env file:

```sh
# Streaming Service Type
STREAMING_SERVICE=none  # Options: "jetstream", "none"

# NATS JetStream Configuration
SUBJECT=TEST_SUBJECT
STREAM_NAME=TEST_STREAM
CONSUMER_NAME=TEST_CONSUMER
DURABLE_NAME=TEST_DURABLE
NATS_URL=localhost:4222

```
