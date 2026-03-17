package postgres_time_traversal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/streamingfast/substreams-foundational-store/store"
)

type Store struct {
	db                        *sqlx.DB
	schemaName                string
	typeUrl                   string
	insertStatement           *sqlx.Stmt
	selectStatement           *sqlx.Stmt
	selectAnyStatement        *sqlx.Stmt
	selectKeyOnlyStatement    *sqlx.Stmt
	selectAllKeyOnlyStatement *sqlx.Stmt
	selectFirstStmt           *sqlx.Stmt
}

func NewStore(dsn *store.DSN, typeUrl string) (*Store, error) {
	ctx := context.Background()
	connectionString := dsn.ConnString()

	db, err := sqlx.Open(dsn.Driver(), connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to postgres: %w", err)
	}

	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(200)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	err = runDatabaseScript(ctx, db, dsn.Schema())
	if err != nil {
		return nil, fmt.Errorf("failed to execute script: %w", err)
	}

	s := &Store{
		db:         db,
		schemaName: dsn.Schema(),
		typeUrl:    typeUrl,
	}

	err = s.prepareStatements()
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	return s, nil
}

func (s *Store) prepareStatements() error {
	// Insert statement remains the same
	insertEntry := fmt.Sprintf(`insert into %s.entries (block_number, key, value, create_time) values ($1,$2,$3,$4);`, s.schemaName)

	insertStatement, err := s.db.Preparex(insertEntry)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for insert entries %q: %w", insertEntry, err)
	}
	s.insertStatement = insertStatement

	// Time traversal select: find the entry with the highest block number <= requested block number
	selectEntry := fmt.Sprintf(`
		select * from %s.entries
		where key = $1 and block_number <= $2
		order by block_number desc
		limit 1;`, s.schemaName)
	selectStatement, err := s.db.Preparex(selectEntry)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select entry %q: %w", selectEntry, err)
	}
	s.selectStatement = selectStatement

	// Time traversal select for multiple keys
	selectAny := fmt.Sprintf(`
		select distinct on (key) * from %s.entries
		where key = any($1) and block_number <= $2
		order by key, block_number desc;`, s.schemaName)
	selectAnyStatement, err := s.db.Preparex(selectAny)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select any %q: %w", selectAny, err)
	}
	s.selectAnyStatement = selectAnyStatement

	// Key-only select: returns only the key (no value) for the highest block <= requested block
	selectKeyOnly := fmt.Sprintf(`
		select key, block_number from %s.entries
		where key = $1 and block_number <= $2
		order by block_number desc
		limit 1;`, s.schemaName)
	selectKeyOnlyStatement, err := s.db.Preparex(selectKeyOnly)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select key only %q: %w", selectKeyOnly, err)
	}
	s.selectKeyOnlyStatement = selectKeyOnlyStatement

	// Key-only select for multiple keys
	selectAllKeyOnly := fmt.Sprintf(`
		select distinct on (key) key, block_number from %s.entries
		where key = any($1) and block_number <= $2
		order by key, block_number desc;`, s.schemaName)
	selectAllKeyOnlyStatement, err := s.db.Preparex(selectAllKeyOnly)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select all key only %q: %w", selectAllKeyOnly, err)
	}
	s.selectAllKeyOnlyStatement = selectAllKeyOnlyStatement

	// Prepare GetFirst: first key >= $1 with oldest block for that key
	selectFirst := fmt.Sprintf(`
		select * from %s.entries
		where key >= $1
		order by key asc, block_number asc
		limit 1;`, s.schemaName)
	selectFirstStmt, err := s.db.Preparex(selectFirst)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select first %q: %w", selectFirst, err)
	}
	s.selectFirstStmt = selectFirstStmt

	return nil
}

// GetTypeURL returns the type URL for the stored values
func (s *Store) GetTypeURL() string {
	return s.typeUrl
}

// Close closes the database connection
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
