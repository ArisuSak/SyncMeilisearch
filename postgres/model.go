package postgres

import "encoding/json"

type WALChange struct {
    Kind         string          `json:"kind"`
    Schema       string          `json:"schema"`
    Table        string          `json:"table"`
    ColumnNames  []string        `json:"columnnames,omitempty"`
    ColumnTypes  []string        `json:"columntypes,omitempty"`
    ColumnValues json.RawMessage   `json:"columnvalues,omitempty"`
    OldKeys      *WALOldKeys     `json:"oldkeys,omitempty"`
}

type WALOldKeys struct {
    KeyNames  []string        `json:"keynames"`
    KeyTypes  []string        `json:"keytypes"`
    KeyValues json.RawMessage   `json:"keyvalues"`
}

type WALData struct {
    Change []WALChange `json:"change"`
}