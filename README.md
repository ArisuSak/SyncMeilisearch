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

Important environment variables:

```sh
# NATS JetStream
STREAMING_SERVICE=none      # option "jetstream, none"

# PostgreSQL connection info
PG_USER=your_db_user
PG_PASSWORD=your_db_password
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database_name

# NATS connection URL
NATS_URL=nats://localhost:4222

# Meilisearch
MEILI_BASE_URL=http://localhost:7700
MEILI_API_KEY=your_meili_api_key
MEILI_TABLE=your_table_name
MEILI_INDEX=your_meili_index

```
