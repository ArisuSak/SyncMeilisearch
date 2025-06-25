package postgres

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

var (
	database *ApplicationConfig
)

var (
	Host     string
	Port     string
	Database string
	User     string
	Password string
)

func init() {
 	data, err := os.ReadFile("config.yaml")
    if err != nil {
        log.Fatalf("Failed to read YAML file: %v", err)
    }

    var config ApplicationConfig
    err = yaml.Unmarshal(data, &config)
    if err != nil {
        log.Fatalf("Failed to parse YAML file: %v", err)
    }

    database = &config

	Host = database.Database.Host
    Port = database.Database.Port
    Database = database.Database.Name
    User = database.Database.User
    Password = database.Database.Password
}


type Store struct {
	Pool   *pgxpool.Pool
	Logger *log.Logger
	DSN    string
}

func New(ctx context.Context, logger *log.Logger) *Store {

	fmt.Println("Connecting to PostgreSQL database...",Host, Port, Database, User)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?application_name=pylon&sslmode=disable", User, Password, Host, Port, Database)
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

func StartReplicationDatabase(ctx context.Context, js nats.JetStreamContext, jetstreamSubject string, callback func([]byte),  tableName []string, l *log.Logger) {
	applicationName := "replication"
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?replication=database&application_name=%s&sslmode=disable",
		User, Password, Host, Port, Database, applicationName,
	)
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		l.Fatal("error connecting to postgres", zap.Error(err))
	}
	defer conn.Close(ctx)

	setupReplication(ctx, conn, tableName, l)

	sysident, err := IdentifySystem(ctx, conn)
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

			// l.Println("wal2json data", zap.String("data", string(xld.WALData)))

			callback(xld.WALData)

			// l.Println("WAL data sent to channel", zap.String("data", string(xld.WALData)))

			if js != nil {
				_, errPub := js.PublishAsync(jetstreamSubject, xld.WALData)
				if errPub != nil {
					l.Fatal("Failed to publish WAL data to NATS", zap.Error(errPub))
				} else {
					l.Print("Successfully initiated async publish of WAL data to NATS", zap.String("data", string(xld.WALData)))
				}
			} else {
				l.Print("JetStream is disabled; skipping publish")
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}

func setupReplication(ctx context.Context, conn *pgconn.PgConn, tableName []string, l *log.Logger) {
	if len(tableName) == 0 {
		l.Println("No tables provided for replication setup")
		return
	}

	// Join table names into a comma-separated string
	tables := strings.Join(tableName, ", ")

	query := fmt.Sprintf("CREATE PUBLICATION replication_demo FOR TABLE %s;", tables)
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
