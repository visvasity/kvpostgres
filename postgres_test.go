// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	ctx := context.Background()

	dbDir := filepath.Join(t.TempDir(), "database")
	t.Log("using database dir", dbDir)

	db, err := New(ctx, dbDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Use the DB
	txn, err := db.NewTransaction(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	txn.Rollback(ctx)
}

func TestBasic(t *testing.T) {
	ctx := context.Background()

	dbDir := filepath.Join(t.TempDir(), "database")
	t.Log("using database dir", dbDir)

	db, err := New(ctx, dbDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if err := tx.Set(ctx, "/1", strings.NewReader("one")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "/2", strings.NewReader("two")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "/3", strings.NewReader("three")); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Discard(ctx)

	valueReader, err := snap.Get(ctx, "/1")
	if err != nil {
		t.Fatal(err)
	}
	valueBytes, err := io.ReadAll(valueReader)
	if err != nil {
		t.Fatal(err)
	}
	if s := string(valueBytes); s != "one" {
		t.Fatalf("wanted one, got %q", s)
	}

	for k, v := range snap.Ascend(ctx, "", "", &err) {
		var sb strings.Builder
		if _, err := io.Copy(&sb, v); err != nil {
			t.Fatal(err)
		}
		t.Log(k, sb.String())
	}

	t.Logf("%s", valueBytes)
}
