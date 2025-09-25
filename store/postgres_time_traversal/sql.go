package postgres_time_traversal

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const createSchema = `CREATE SCHEMA IF NOT EXISTS %s;`
const createEntriesTable = `
create table if not exists %s.entries
(
    block_number bigint    not null,
    key          bytea     not null,
    value        bytea,
    create_time  timestamp not null,
    constraint entries_pk
        primary key (block_number, key)
);

create index if not exists entries_key_index
    on %s.entries (key);

create index if not exists entries_block_index
    on %s.entries (block_number);

create index if not exists entries_key_block_index
    on %s.entries (key, block_number desc);
`

func runDatabaseScript(ctx context.Context, db *sqlx.DB, schemaName string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	_, err = tx.Exec(fmt.Sprintf(createSchema, schemaName))
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create schema: %w", err)
	}

	_, err = tx.Exec(fmt.Sprintf(createEntriesTable, schemaName, schemaName, schemaName, schemaName))
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create entries table: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
