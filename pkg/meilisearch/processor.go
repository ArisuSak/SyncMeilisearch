package meilisearch

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"nats-jetstream/pkg/postgres"
)

type DefaultMeilisearchProcessor[T any] struct{
	PrimaryKey string
}

func (p *DefaultMeilisearchProcessor[T]) preparePayload(change postgres.WALChange) ([]byte, error) {
	payload := make(map[string]interface{})

	var columnValues []interface{}
	if err := json.Unmarshal(change.ColumnValues, &columnValues); err != nil {
		return nil, fmt.Errorf("failed to unmarshal column values: %w", err)
	}

	for i, colName := range change.ColumnNames {
		if i < len(columnValues) {
			payload[colName] = columnValues[i]
		}
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload into JSON: %w", err)
	}

	return jsonPayload, nil
}
func (p *DefaultMeilisearchProcessor[T]) extractIDFromChange(change postgres.WALChange) (string, error) {
    if changeJson, err := json.MarshalIndent(change, "", "  "); err == nil {
        fmt.Println("extractIDFromChange called with change:", string(changeJson))
    } else {
        fmt.Println("Failed to marshal change:", err)
    }

    if change.OldKeys == nil {
        return "", fmt.Errorf("oldkeys field is missing")
    }

    // Since KeyValues is json.RawMessage, unmarshal it properly
    var keyValues []interface{}
    if err := json.Unmarshal(change.OldKeys.KeyValues, &keyValues); err != nil {
        return "", fmt.Errorf("failed to unmarshal key values: %w", err)
    }

    fmt.Printf("Debug: Unmarshaled keyValues: %+v (type: %T)\n", keyValues, keyValues)

    for i, keyName := range change.OldKeys.KeyNames {
        if keyName == p.PrimaryKey {
            if i >= len(keyValues) {
                return "", fmt.Errorf("primary key index out of range")
            }
            
            idValue := keyValues[i]
            stringID := fmt.Sprintf("%v", idValue)
            
            fmt.Printf("Debug: Found primary key '%s' at index %d with value: %v (type: %T)\n", keyName, i, idValue, idValue)
            fmt.Printf("Debug: Converted to string: '%s'\n", stringID)
            
            return stringID, nil
        }
    }

    return "", fmt.Errorf("primary key %q not found in oldkeys", p.PrimaryKey)
}

func (m *MeiliSearchHandler) fetchDataFromDatabase(db *sql.DB) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", m.TableName)
	rows, err := db.Query(query)

	if err != nil {
		return nil, fmt.Errorf("failed to query from Database: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	var documents []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		doc := make(map[string]interface{})
		for i, col := range columns {
			doc[col] = values[i]
		}
		documents = append(documents, doc)
	}

	return documents, nil
}
