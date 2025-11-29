// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lib/pq"
)

const defaultDB = "kvs"

type Database struct {
	db    *sql.DB
	stopf func()
}

type Transaction struct {
	db *Database
	tx *sql.Tx
}

// New creates a key-value store (if it doesn't exist) backed by a private
// postgres instance.
func New(ctx context.Context, dataDir string) (_ *Database, status error) {
	if !filepath.IsAbs(dataDir) {
		absDir, err := filepath.Abs(dataDir)
		if err != nil {
			return nil, err
		}
		dataDir = absDir
	}

	stopf, err := Start(ctx, dataDir)
	if err != nil {
		return nil, err
	}

	cs := fmt.Sprintf("user=postgres dbname=%s host=%s", defaultDB, dataDir)
	connector, err := pq.NewConnector(cs)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	defer func() {
		if status != nil {
			_ = db.Close()
		}
	}()

	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS kv (key BYTEA PRIMARY KEY, value BYTEA)")
	if _, err := db.ExecContext(ctx, q); err != nil {
		return nil, err
	}
	d := &Database{
		db:    db,
		stopf: stopf,
	}
	return d, nil
}

// Connect creates a db instance using an already running database server at
// the given directory.
func Connect(ctx context.Context, dataDir string) (_ *Database, status error) {
	cs := fmt.Sprintf("user=postgres dbname=%s host=%s", defaultDB, dataDir)
	connector, err := pq.NewConnector(cs)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	defer func() {
		if status != nil {
			_ = db.Close()
		}
	}()

	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS kv (key BYTEA PRIMARY KEY, value BYTEA)")
	if _, err := db.ExecContext(ctx, q); err != nil {
		return nil, err
	}
	d := &Database{
		db: db,
	}
	return d, nil
}

// Close shuts down the postgres database server.
func (d *Database) Close() error {
	if d.stopf != nil {
		d.stopf()
		d.stopf = nil
	}
	return nil
}

// NewSnapshot creates a read-only snapshot of the key-value database.
func (d *Database) NewSnapshot(ctx context.Context) (*Transaction, error) {
	tx, err := d.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		tx.Rollback()
		return nil, err
	}

	s := &Transaction{
		db: d,
		tx: tx,
	}
	return s, nil
}

// Discard releases a snapshot.
func (t *Transaction) Discard(ctx context.Context) error {
	return t.Rollback(context.Background())
}

// NewTransaction creates a new transaction.
func (d *Database) NewTransaction(ctx context.Context) (*Transaction, error) {
	tx, err := d.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		tx.Rollback()
		return nil, err
	}

	t := &Transaction{
		db: d,
		tx: tx,
	}
	return t, nil
}

// Commit commits a transaction.
func (t *Transaction) Commit(ctx context.Context) error {
	if t.tx == nil {
		return os.ErrClosed
	}
	defer func() {
		t.tx = nil
	}()

	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

// Rollback drops a transaction.
func (t *Transaction) Rollback(ctx context.Context) error {
	if t.tx == nil {
		return os.ErrClosed
	}
	defer func() {
		t.tx = nil
	}()

	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("could not rollback transaction: %w", err)
	}
	return nil
}

// Get returns the value for a given key.
func (t *Transaction) Get(ctx context.Context, k string) (io.Reader, error) {
	if t.tx == nil {
		return nil, os.ErrClosed
	}
	if len(k) == 0 {
		return nil, os.ErrInvalid
	}

	q := "SELECT value FROM kv WHERE key = $1"
	row := t.tx.QueryRowContext(ctx, q, k)

	var v string
	if err := row.Scan(&v); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return strings.NewReader(v), nil
}

// Set creates or updates a key-value pair.
func (t *Transaction) Set(ctx context.Context, k string, v io.Reader) error {
	if t.tx == nil {
		return os.ErrClosed
	}
	if v == nil || len(k) == 0 {
		return os.ErrInvalid
	}
	s, err := io.ReadAll(v)
	if err != nil {
		return err
	}
	q := `INSERT INTO kv (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;`
	if _, err := t.tx.ExecContext(ctx, q, k, s); err != nil {
		return err
	}
	return nil
}

// Delete removes a key-value pair.
func (t *Transaction) Delete(ctx context.Context, k string) error {
	if t.tx == nil {
		return os.ErrClosed
	}
	if len(k) == 0 {
		return os.ErrInvalid
	}

	q := "DELETE FROM kv WHERE key = $1"
	result, err := t.tx.ExecContext(ctx, q, k)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return os.ErrNotExist
		}
		return err
	}

	nrows, err := result.RowsAffected()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return os.ErrNotExist
		}
		return err
	}
	if nrows == 0 {
		return os.ErrNotExist
	}
	return nil
}

func (t *Transaction) Ascend(ctx context.Context, beg, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if t.tx == nil {
			*errp = os.ErrClosed
			return
		}
		if beg > end && end != "" {
			*errp = os.ErrInvalid
			return
		}

		// NOTE: Cursor name cannot be passed as a value parameter using $1 syntax.
		name := fmt.Sprintf("c%d", time.Now().UnixNano())
		cursorPrefix := fmt.Sprintf("DECLARE %s CURSOR FOR ", name)

		switch {
		case beg == "" && end == "":
			q := cursorPrefix + "SELECT key, value FROM kv ORDER BY key ASC"
			if _, err := t.tx.ExecContext(ctx, q); err != nil {
				*errp = err
				return
			}

		case beg != "" && end != "":
			q := cursorPrefix + "SELECT key, value FROM kv WHERE key >= $1 AND key < $2 ORDER BY key ASC"
			if _, err := t.tx.ExecContext(ctx, q, beg, end); err != nil {
				*errp = err
				return
			}

		case beg == "" && end != "":
			q := cursorPrefix + "SELECT key, value FROM kv WHERE key < $1 ORDER BY key ASC"
			if _, err := t.tx.ExecContext(ctx, q, end); err != nil {
				*errp = err
				return
			}

		case beg != "" && end == "":
			q := cursorPrefix + "SELECT key, value FROM kv WHERE key >= $1 ORDER BY key ASC"
			if _, err := t.tx.ExecContext(ctx, q, beg); err != nil {
				*errp = err
				return
			}
		}
		defer t.tx.Exec("CLOSE " + name)

		for {
			var key, value string
			if err := t.tx.QueryRowContext(ctx, `FETCH NEXT FROM `+name).Scan(&key, &value); err != nil {
				if err == sql.ErrNoRows {
					break
				}
				*errp = err
				return
			}
			if !yield(key, strings.NewReader(value)) {
				return
			}
		}
	}
}

// Descend returns key-value pairs in a given range, in descending order.
func (t *Transaction) Descend(ctx context.Context, beg, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if t.tx == nil {
			*errp = os.ErrClosed
			return
		}
		if beg > end && end != "" {
			*errp = os.ErrInvalid
			return
		}

		// NOTE: Cursor name cannot be passed as a value parameter using $1 syntax.
		name := fmt.Sprintf("c%d", time.Now().UnixNano())
		cursorPrefix := fmt.Sprintf("DECLARE %s CURSOR FOR ", name)

		switch {
		case beg == "" && end == "":
			q := cursorPrefix + "SELECT key, value FROM kv ORDER BY key DESC"
			if _, err := t.tx.ExecContext(ctx, q); err != nil {
				*errp = err
				return
			}

		case beg != "" && end != "":
			q := cursorPrefix + "SELECT key, value FROM kv WHERE key >= $1 AND key < $2 ORDER BY key DESC"
			if _, err := t.tx.ExecContext(ctx, q, beg, end); err != nil {
				*errp = err
				return
			}

		case beg == "" && end != "":
			q := cursorPrefix + "SELECT key, value FROM kv WHERE key < $1 ORDER BY key DESC"
			if _, err := t.tx.ExecContext(ctx, q, end); err != nil {
				*errp = err
				return
			}

		case beg != "" && end == "":
			q := cursorPrefix + "SELECT key, value FROM kv WHERE key >= $1 ORDER BY key DESC"
			if _, err := t.tx.ExecContext(ctx, q, beg); err != nil {
				*errp = err
				return
			}
		}
		defer t.tx.Exec("CLOSE " + name)

		for {
			var key, value string
			if err := t.tx.QueryRowContext(ctx, `FETCH NEXT FROM `+name).Scan(&key, &value); err != nil {
				if err == sql.ErrNoRows {
					break
				}
				*errp = err
				return
			}
			if !yield(key, strings.NewReader(value)) {
				return
			}
		}
	}
}
