package postgres

import (
	"context"
	"fmt"
	"log"
	"nats-jetstream/nat"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

const (
	Host     = "localhost"
	Port     = 5437
	Database = "pylon"
	User     = "postgres"
	Password = "sonoftruth"
	TableName = "tenants"
)


type Store struct {
  Pool   *pgxpool.Pool
  Logger *log.Logger
  DSN    string
}

func New(ctx context.Context, logger *log.Logger) (*Store) {
  dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?application_name=pylon&sslmode=disable", User, Password, Host, Port, Database)
  cfg, err := pgxpool.ParseConfig(dsn)
  if err != nil {
    logger.Fatal("error parsing configuration", zap.Error(err))
  }

  cfg.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
    fmt.Print("acquring connection to postgres")
    return true
  }

  cfg.AfterRelease = func(c *pgx.Conn) bool {
    fmt.Print("connection to postgres released")
    return true
  }

  cfg.BeforeClose = func(c *pgx.Conn) {
    fmt.Print("connection to postgres closed")
  }

  pool, err := pgxpool.NewWithConfig(ctx, cfg)
  if err != nil {
    return nil
  }

  store := Store{
    Pool:   pool,
    Logger: logger,
    DSN:    dsn,
  }

  return &store
}

func StartReplicationDatabase(ctx context.Context, js nats.JetStreamContext, jetstreamSubject string, l *log.Logger) {
    dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database&application_name=salesquake&sslmode=disable", User, Password, Host, Port, Database)
    conn, err := pgconn.Connect(ctx, dsn)
    if err != nil {
        l.Fatal("error connecting to postgres", zap.Error(err))
    }
    defer conn.Close(ctx)

    setupReplication(ctx, conn, l)

    sysident, err :=IdentifySystem(ctx, conn)
    if err != nil {
        l.Fatal("IdentifySystem failed", zap.Error(err))
    }
	l.Println("SystemID", zap.String("SystemID", sysident.SystemID), zap.Uint32("Timeline", uint32(sysident.Timeline)), zap.String("XLogPos", sysident.XLogPos.String()), zap.String("DBName", sysident.DBName))

    slotName := "replication_demo"
    pluginArguments := []string{"\"pretty-print\" 'true'"}

    err = StartReplication(ctx, conn, slotName, sysident.XLogPos, StartReplicationOptions{PluginArgs: pluginArguments})
    if err != nil {
        l.Fatal("StartReplication failed", zap.Error(err))
    }
	l.Println("Logical replication started on slot", zap.String("slotName", slotName))

    clientXLogPos := sysident.XLogPos
    standbyMessageTimeout := time.Second * 10
    nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	for {
        if time.Now().After(nextStandbyMessageDeadline) {
            err := SendStandbyStatusUpdate(ctx, conn, StandbyStatusUpdate{WALWritePosition: clientXLogPos})
            if err != nil {
                l.Fatal("SendStandbyStatusUpdate failed", zap.Error(err))
            }
			l.Println("Sent Standby status message", zap.String("WALWritePosition", clientXLogPos.String()))
            nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
        }

        ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
        rawMsg, err := conn.ReceiveMessage(ctx)
        cancel()
        if err != nil {
            if pgconn.Timeout(err) {
                continue
            }
            l.Fatal("ReceiveMessage failed", zap.Error(err))
        }

        if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
            l.Fatal("received Postgres WAL error", zap.Any("error", errMsg))
        }

        msg, ok := rawMsg.(*pgproto3.CopyData)
        if !ok {
            l.Println("Received unexpected message", zap.Any("message", rawMsg))
            continue
        }

        switch msg.Data[0] {
        case PrimaryKeepaliveMessageByteID:
            pkm, err := ParsePrimaryKeepaliveMessage(msg.Data[1:])
            if err != nil {
                l.Fatal("ParsePrimaryKeepaliveMessage failed", zap.Error(err))
            }

            l.Println("Primary Keepalive Message", zap.String("ServerWALEnd", pkm.ServerWALEnd.String()), zap.Time("ServerTime", pkm.ServerTime), zap.Bool("ReplyRequested", pkm.ReplyRequested))
            if pkm.ServerWALEnd > clientXLogPos {
                clientXLogPos = pkm.ServerWALEnd
            }

            if pkm.ReplyRequested {
                nextStandbyMessageDeadline = time.Time{}
            }

		case XLogDataByteID:
            xld, err := ParseXLogData(msg.Data[1:])
            if err != nil {
                l.Fatal("ParseXLogData failed", zap.Error(err))
            }
	
            l.Println("wal2json data", zap.String("data", string(xld.WALData)))
            
            errPub := nat.PublishAsync(js, jetstreamSubject, xld.WALData)
            if errPub != nil {
                l.Fatal("Failed to publish WAL data to NATS", zap.Error(errPub))
            } else {
                l.Print("Successfully published WAL data to NATS", zap.String("data", string(xld.WALData)))
            }

            if xld.WALStart > clientXLogPos {
                clientXLogPos = xld.WALStart
            }
        }
    }
}

func setupReplication(ctx context.Context, conn *pgconn.PgConn, l *log.Logger) {
	query := fmt.Sprintf("CREATE PUBLICATION replication_demo FOR TABLE %s;", TableName)
	result := conn.Exec(ctx, query)
	
    _, err := result.ReadAll()
    if err != nil {
        if strings.Contains(err.Error(), "publication \"replication_demo\" already exists") {
			l.Println("publication replication_demo already exists")
        } else {
            l.Fatal("create publication error", zap.Error(err))
        }
    } else {
		l.Println("create publication replication_demo")
    }
    result = conn.Exec(ctx, "SELECT pg_drop_replication_slot('replication_demo');")
    _, err = result.ReadAll()
    if err != nil && !strings.Contains(err.Error(), "does not exist") {
        l.Fatal("drop replication slot error", zap.Error(err))
    } else if err == nil {
        l.Println("dropped existing replication slot replication_demo")
    }

    result = conn.Exec(ctx, "SELECT * FROM pg_create_logical_replication_slot('replication_demo', 'wal2json');")
    _, err = result.ReadAll()
    if err != nil {
        l.Fatal("create replication slot error", zap.Error(err))
    }
    l.Println("created new replication slot replication_demo")
}

