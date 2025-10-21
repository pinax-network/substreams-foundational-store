package postgres

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/streamingfast/substreams-foundational-store/store"
)

type Store struct {
	db                 *sqlx.DB
	schemaName         string
	typeUrl            string
	insertStatement    *sqlx.Stmt
	selectStatement    *sqlx.Stmt
	selectAnyStatement *sqlx.Stmt
	selectFirstStmt    *sqlx.Stmt
}

func NewStore(dsn *store.DSN, typeUrl string) (*Store, error) {
	ctx := context.Background()
	connectionString := dsn.ConnString()

	db, err := sqlx.Open(dsn.Driver(), connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to postgres: %w", err)
	}

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
	insertEntry := fmt.Sprintf(`insert into %s.entries (block_number, key, value, create_time) values ($1,$2,$3,$4);`, s.schemaName)

	insertStatement, err := s.db.Preparex(insertEntry)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for insert entries %q: %w", insertEntry, err)
	}
	s.insertStatement = insertStatement

	selectEntry := fmt.Sprintf(`select * from %s.entries where key = $1;`, s.schemaName)
	selectStatement, err := s.db.Preparex(selectEntry)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select entry %q: %w", selectEntry, err)
	}
	s.selectStatement = selectStatement

	selectAny := fmt.Sprintf(`SELECT * FROM %s.entries WHERE key = ANY($1);`, s.schemaName)
	selectAnyStatement, err := s.db.Preparex(selectAny)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select any %q: %w", selectAny, err)
	}
	s.selectAnyStatement = selectAnyStatement

	// Prepare GetFirst statement: first key >= $1
	selectFirst := fmt.Sprintf(`SELECT * FROM %s.entries WHERE key >= $1 ORDER BY key ASC LIMIT 1;`, s.schemaName)
	selectFirstStmt, err := s.db.Preparex(selectFirst)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for select first %q: %w", selectFirst, err)
	}
	s.selectFirstStmt = selectFirstStmt

	return nil
}
