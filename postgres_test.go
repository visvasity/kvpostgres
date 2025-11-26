// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/lib/pq"
)

func TestConnect(t *testing.T) {
	name := "user=postgres dbname=default host=/tmp/"
	connector, err := pq.NewConnector(name)
	if err != nil {
		fmt.Println(err)
		return
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// Use the DB
	txn, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return
	}
	txn.Rollback()
}

func TestBasic(t *testing.T) {
	ctx := context.Background()
	db, err := New(ctx, "user=postgres dbname=default host=/tmp/", nil)
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
