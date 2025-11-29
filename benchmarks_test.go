// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvbenchmarks"
)

func BenchmarkPostgres(b *testing.B) {
	ctx := context.Background()

	dbDir := filepath.Join(b.TempDir(), "database")
	b.Log("using database dir", dbDir)

	pg, err := New(ctx, dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer pg.Close()

	db := kv.DatabaseFrom(pg)
	if db == nil {
		b.Fatal("failed to open database")
	}

	kvbenchmarks.RunBenchmarks(ctx, b, db)
}
