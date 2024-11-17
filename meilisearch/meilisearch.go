package meilisearch

import (
	"encoding/json"
	"fmt"
	"nats-jetstream/postgres"
)

func prepareMeilisearchPayload[T any](change postgres.WALChange) ([]byte, error) {
	payload := make(map[string]T)

	var columnValues []interface{}
	if err := json.Unmarshal(change.ColumnValues, &columnValues); err != nil {
		return nil, fmt.Errorf("failed to unmarshal column values: %w", err)
	}

	for i, colName := range change.ColumnNames {
		if i < len(columnValues) {
			if value, ok := columnValues[i].(T); ok {
				payload[colName] = value
			}
		}
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload into JSON: %w", err)
	}

	return jsonPayload, nil
}

func extractIDFromChange[T any](change postgres.WALChange) (T, error) {
	var zeroID T

	if change.OldKeys != nil {
		return zeroID, fmt.Errorf("oldkeys field is missing")
	}

	var keyValues []interface{}
	if err := json.Unmarshal(change.OldKeys.KeyValues, &keyValues); err != nil {
		return zeroID, fmt.Errorf("failed to unmarshal key values: %w", err)
	}

	for i, keyName := range change.OldKeys.KeyNames {
		if keyName == "id" && i < len(keyValues) {
			if i < len(keyValues) {
				if id, ok := keyValues[i].(T); ok {
					return id, nil
				} else {
					return zeroID, fmt.Errorf("ID is not of expected type")
				}
			}
		}
	}
	return zeroID, fmt.Errorf("ID column not found in oldkeys")
}

// func fetchDataFromDatabase(db *sql.DB) ([]map[string]interface{}, error) {
//     rows, err := db.Query("SELECT * FROM main.tenants")

//     if err != nil {
//         return nil, fmt.Errorf("failed to query from Database: %v", err)
//     }
//     defer rows.Close()

//     columns, err := rows.Columns()
//     if err != nil {
//         return nil, fmt.Errorf("failed to get columns: %v", err)
//     }

//     var documents []map[string]interface{}
//     for rows.Next() {
//         values := make([]interface{}, len(columns))
//         valuePtrs := make([]interface{}, len(columns))
//         for i := range values {
//             valuePtrs[i] = &values[i]
//         }

//         if err := rows.Scan(valuePtrs...); err != nil {
//             return nil, fmt.Errorf("failed to scan row: %v", err)
//         }

//         doc := make(map[string]interface{})
//         for i, col := range columns {
//             doc[col] = values[i]
//         }
//         documents = append(documents, doc)
//     }

//     return documents, nil
// }
