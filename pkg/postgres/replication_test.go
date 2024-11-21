package postgres

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const outputPlugin = "wal2json"

func TestReadMessages(t *testing.T) {
    ctx := context.Background()
	//conn, err := pgconn.Connect(ctx,"postgres://postgres:sonoftruth@localhost:5437/{here}?replication=database&application_name=pylon")
    conn, err := pgconn.Connect(ctx,"postgres://postgres:sonoftruth@localhost:5437/pylon?replication=database&application_name=pylon")
    if err != nil {
        t.Fatal("error connecting to postgres", err)
    }
    defer conn.Close(ctx)

    result := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS replication_demo;")
    _, err = result.ReadAll()
    if err != nil {
        log.Fatalln("drop publication if exists error", err)
    }

    result = conn.Exec(ctx, "CREATE PUBLICATION replication_demo FOR ALL TABLES;")
    _, err = result.ReadAll()
    if err != nil {
        log.Fatalln("create publication error", err)
    }
    log.Println("create publication replication_demo")

    // Drop existing replication slot if it exists
    result = conn.Exec(ctx, "SELECT pg_drop_replication_slot('replication_demo') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replication_demo');")
    _, err = result.ReadAll()
    if err != nil {
        log.Fatalln("drop replication slot error", err)
    }
    log.Println("dropped existing replication slot replication_demo")

    // Create new replication slot
    result = conn.Exec(ctx, "SELECT * FROM pg_create_logical_replication_slot('replication_demo', 'wal2json');")
    _, err = result.ReadAll()
    if err != nil {
        log.Fatalln("create replication slot error", err)
    }
    log.Println("create replication slot replication_demo")

    var pluginArguments []string
    if outputPlugin == "pgoutput" {
        pluginArguments = []string{
            "proto_version '2'",
            "publication_names 'replication_demo'",
            "messages 'true'",
            "streaming 'true'",
        }
    } else if outputPlugin == "wal2json" {
        pluginArguments = []string{"\"pretty-print\" 'true'"}
    }

    sysident, err := IdentifySystem(ctx, conn)
    if err != nil {
        log.Fatalln("IdentifySystem failed:", err)
    }
    log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

    slotName := "replication_demo"
    err = StartReplication(ctx, conn, slotName, sysident.XLogPos, StartReplicationOptions{PluginArgs: pluginArguments})
    if err != nil {
        log.Fatalln("StartReplication failed:", err)
    }
    log.Println("Logical replication started on slot", slotName)

    // Create a separate connection for the INSERT operation
    insertConn, err := pgconn.Connect(ctx,"postgres://postgres:sonoftruth@localhost:5437/pylon?replication=database&application_name=pylon")
    if err != nil {
        t.Fatal("error connecting to postgres for insert operation", err)
    }
    defer insertConn.Close(ctx)

    // Insert data into main.tenants table
    insertResult := insertConn.Exec(ctx, "INSERT INTO main.tenants (name) VALUES ('test');")
    _, err = insertResult.ReadAll()
    if err != nil {
        log.Fatalln("insert into main.tenants error", err)
    }
    log.Println("Inserted data into main.tenants table")

    // Commit the transaction to ensure the changes are written to WAL
    commitResult := insertConn.Exec(ctx, "COMMIT;")
    _, err = commitResult.ReadAll()
    if err != nil {
        log.Fatalln("commit transaction error", err)
    }
    log.Println("Committed transaction")

    clientXLogPos := sysident.XLogPos
    standbyMessageTimeout := time.Second * 10
    nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
    relationsV2 := map[uint32]*RelationMessageV2{}
    typeMap := pgtype.NewMap()

    inStream := false

loop:
    for timeout := time.After(15 * time.Second); ; {
        select {
        case <-timeout:
            break loop
        default:
        }

        if time.Now().After(nextStandbyMessageDeadline) {
            err := SendStandbyStatusUpdate(context.Background(), conn, StandbyStatusUpdate{WALWritePosition: clientXLogPos})
            if err != nil {
                log.Fatalln("SendStandbyStatusUpdate failed:", err)
            }
            log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
            nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
        }

        ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
        rawMsg, err := conn.ReceiveMessage(ctx)
        cancel()
        if err != nil {
            if pgconn.Timeout(err) {
                continue
            }
            log.Fatalln("ReceiveMessage failed:", err)
        }

        if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
            log.Fatalf("received Postgres WAL error: %+v", errMsg)
        }

        msg, ok := rawMsg.(*pgproto3.CopyData)
        if !ok {
            log.Printf("Received unexpected message: %T\n", rawMsg)
            continue
        }

        switch msg.Data[0] {
        case PrimaryKeepaliveMessageByteID:
            pkm, err := ParsePrimaryKeepaliveMessage(msg.Data[1:])
            if err != nil {
                log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
            }

            log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
            if pkm.ServerWALEnd > clientXLogPos {
                clientXLogPos = pkm.ServerWALEnd
            }

            if pkm.ReplyRequested {
                nextStandbyMessageDeadline = time.Time{}
            }

        case XLogDataByteID:
            xld, err := ParseXLogData(msg.Data[1:])
            if err != nil {
                log.Fatalln("ParseXLogData failed:", err)
            }

            log.Println("Received XLogData message")

            if outputPlugin == "wal2json" {
                t.Logf("wal2json data: %s\n", string(xld.WALData))
            } else {
                log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)
                processV2(xld.WALData, relationsV2, typeMap, &inStream)
            }

            if xld.WALStart > clientXLogPos {
                clientXLogPos = xld.WALStart
            }
        }
    }
}

func processV2(walData []byte, relations map[uint32]*RelationMessageV2, typeMap *pgtype.Map, inStream *bool) {
    logicalMsg, err := ParseV2(walData, *inStream)
    if err != nil {
        log.Fatalf("Parse logical replication message: %s", err)
    }

    log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
    switch logicalMsg := logicalMsg.(type) {
    case *RelationMessageV2:
        relations[logicalMsg.RelationID] = logicalMsg

    case *BeginMessage:
        // Indicates the beginning of a group of changes in a transaction. This
        // is only sent for committed transactions. You won't get any events
        // from rolled back transactions.

    case *CommitMessage:

    case *InsertMessageV2:
        rel, ok := relations[logicalMsg.RelationID]
        if !ok {
            log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
        }
        values := map[string]interface{}{}
        for idx, col := range logicalMsg.Tuple.Columns {
            colName := rel.Columns[idx].Name
            switch col.DataType {
            case 'n': // null
                values[colName] = nil
            case 'u': // unchanged toast
                // This TOAST value was not changed. TOAST values are not stored
                // in the tuple, and logical replication doesn't want to spend a
                // disk read to fetch its value for you.
            case 't': //text
                val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
                if err != nil {
                    log.Fatalln("error decoding column data:", err)
                }
                values[colName] = val
            }
        }
        log.Printf("insert for xid %d\n", logicalMsg.Xid)
        log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

    case *UpdateMessageV2:
        log.Printf("update for xid %d\n", logicalMsg.Xid)
        // ...
    case *DeleteMessageV2:
        log.Printf("delete for xid %d\n", logicalMsg.Xid)
        // ...
    case *TruncateMessageV2:
        log.Printf("truncate for xid %d\n", logicalMsg.Xid)
        // ...

    case *TypeMessageV2:
    case *OriginMessage:

    case *LogicalDecodingMessageV2:
        log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

    case *StreamStartMessageV2:
        *inStream = true
        log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
    case *StreamStopMessageV2:
        *inStream = false
        log.Printf("Stream stop message")
    case *StreamCommitMessageV2:
        log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
    case *StreamAbortMessageV2:
        log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
    default:
        log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
    }
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
    if dt, ok := mi.TypeForOID(dataType); ok {
        return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
    }
    return string(data), nil
}