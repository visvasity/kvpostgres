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

	"github.com/lib/pq"
)

const defaultDB = "kvs"

type DB struct {
	db      *sql.DB
	checker func(string) bool
	stopf   func()
}

type Tx struct {
	db *DB
	tx *sql.Tx
}

// New creates a key-value store backed by an sqlite database.
func New(ctx context.Context, dataDir string, checker func(string) bool) (_ *DB, status error) {
	if !filepath.IsAbs(dataDir) {
		absDir, err := filepath.Abs(dataDir)
		if err != nil {
			return nil, err
		}
		dataDir = absDir
	}

	stopf, err := Start(ctx, dataDir, "" /* pgctlBinary */)
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
	d := &DB{
		db:      db,
		checker: checker,
		stopf:   stopf,
	}
	return d, nil
}

// Close shuts down the postgres database server.
func (d *DB) Close() error {
	if d.stopf == nil {
		return os.ErrClosed
	}
	d.stopf()
	d.stopf = nil
	return nil
}

func (d *DB) checkKey(k string) bool {
	if len(k) == 0 {
		return false
	}
	if d.checker == nil {
		return true
	}
	return d.checker(k)
}

// NewSnapshot creates a read-only snapshot of the key-value database.
func (d *DB) NewSnapshot(ctx context.Context) (*Tx, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, err
	}

	s := &Tx{
		db: d,
		tx: tx,
	}
	return s, nil
}

// Discard releases a snapshot.
func (t *Tx) Discard(ctx context.Context) error {
	return t.Rollback(ctx)
}

// NewTransaction creates a new transaction.
func (d *DB) NewTransaction(ctx context.Context) (*Tx, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		return nil, err
	}

	t := &Tx{
		db: d,
		tx: tx,
	}
	return t, nil
}

// Commit commits a transaction.
func (t *Tx) Commit(ctx context.Context) error {
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
func (t *Tx) Rollback(ctx context.Context) error {
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
func (t *Tx) Get(ctx context.Context, k string) (io.Reader, error) {
	if t.tx == nil {
		return nil, os.ErrClosed
	}
	if !t.db.checkKey(k) {
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
func (t *Tx) Set(ctx context.Context, k string, v io.Reader) error {
	if v == nil {
		return os.ErrInvalid
	}
	if t.tx == nil {
		return os.ErrClosed
	}
	if !t.db.checkKey(k) {
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
func (t *Tx) Delete(ctx context.Context, k string) error {
	if t.tx == nil {
		return os.ErrClosed
	}
	if !t.db.checkKey(k) {
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

// Ascend returns key-value pairs in a given range, in ascending order.
func (t *Tx) Ascend(ctx context.Context, beg, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if t.tx == nil {
			*errp = os.ErrClosed
			return
		}
		if beg > end && end != "" {
			*errp = os.ErrInvalid
			return
		}

		var err error
		var rs *sql.Rows

		switch {
		case beg == "" && end == "":
			q := "SELECT key, value FROM kv ORDER BY key ASC"
			rs, err = t.tx.QueryContext(ctx, q)

		case beg != "" && end != "":
			q := "SELECT key, value FROM kv WHERE key >= $1 AND key < $2 ORDER BY key ASC"
			rs, err = t.tx.QueryContext(ctx, q, beg, end)

		case beg == "" && end != "":
			q := "SELECT key, value FROM kv WHERE key < $1 ORDER BY key ASC"
			rs, err = t.tx.QueryContext(ctx, q, end)

		case beg != "" && end == "":
			q := "SELECT key, value FROM kv WHERE key >= $1 ORDER BY key ASC"
			rs, err = t.tx.QueryContext(ctx, q, beg)
		}

		if err != nil {
			*errp = err
			return
		}

		defer rs.Close()

		for rs.Next() {
			if err := rs.Err(); err != nil {
				*errp = err
				return
			}
			var key, value string
			if err := rs.Scan(&key, &value); err != nil {
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
func (t *Tx) Descend(ctx context.Context, beg, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if t.tx == nil {
			*errp = os.ErrClosed
			return
		}
		if beg > end && end != "" {
			*errp = os.ErrInvalid
			return
		}

		var err error
		var rs *sql.Rows

		switch {
		case beg == "" && end == "":
			q := "SELECT key, value FROM kv ORDER BY key DESC"
			rs, err = t.tx.QueryContext(ctx, q)

		case beg != "" && end != "":
			q := "SELECT key, value FROM kv WHERE key >= $1 AND key < $2 ORDER BY key DESC"
			rs, err = t.tx.QueryContext(ctx, q, beg, end)

		case beg == "" && end != "":
			q := "SELECT key, value FROM kv WHERE key < $1 ORDER BY key DESC"
			rs, err = t.tx.QueryContext(ctx, q, end)

		case beg != "" && end == "":
			q := "SELECT key, value FROM kv WHERE key >= $1 ORDER BY key DESC"
			rs, err = t.tx.QueryContext(ctx, q, beg)
		}

		if err != nil {
			*errp = err
			return
		}

		defer rs.Close()

		for rs.Next() {
			if err := rs.Err(); err != nil {
				*errp = err
				return
			}
			var key, value string
			if err := rs.Scan(&key, &value); err != nil {
				*errp = err
				return
			}
			if !yield(key, strings.NewReader(value)) {
				return
			}
		}
	}
}
