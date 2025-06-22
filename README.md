# NATS JetStream Integration

This project demonstrates how to integrate NATS JetStream with PostgreSQL and Meilisearch for seamless data synchronization using Write-Ahead Logs (WAL) and Go.

## Prerequisites

- Go 1.16 or later
- PostgreSQL
- NATS Server
- Meilisearch

## Install Dependencies

```sh
go mod tidy
```

Create a .env file based on the .env.example template and configure your environment variables.

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
