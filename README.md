# NATS JetStream Integration
This project demonstrates how to integrate NATS JetStream with PostgreSQL and Meilisearch for seamless data synchronization using Write-Ahead Logs (WAL) and Go.

## Prerequisites
- Go 1.16 or later
- PostgreSQL
- NATS Server
- Meilisearch

# Install dependencies:
go mod tidy

# Configuration
To configure the application, create a .env file using the .env.example file as a template.

## Run application
```sh
cd nats-jetstream
```
```sh
go run cmd/main.go
```
