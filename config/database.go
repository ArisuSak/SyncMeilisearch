package config

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"nats-jetstream/pkg/postgres"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type DatabaseStruct struct {
    DB     *sql.DB
    Store  *postgres.Store
    Logger *log.Logger
}

func NewDatabase(ctx context.Context, logger *log.Logger) (*DatabaseStruct, error) {
    store := postgres.New(ctx, logger)
    if store == nil {
        return nil, fmt.Errorf("failed to create store")
    }

    logger.Printf("Connection to PostgreSQL successfully")

    db, err := sql.Open("pgx", store.DSN)
    if err != nil {
        return nil, fmt.Errorf("failed to open PostgreSQL with DSN: %w", err)
    }

    return &DatabaseStruct{
        DB:     db,
        Store:  store,
        Logger: logger,
    }, nil
}

func (d *DatabaseStruct) Close() error {
    return d.DB.Close()
}