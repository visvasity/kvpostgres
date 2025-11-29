// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"context"
	"path/filepath"
	"testing"
)

func TestStart(t *testing.T) {
	ctx := context.Background()

	dataDir := filepath.Join(t.TempDir(), "database")

	stop1, err := Start(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer stop1()

	stop2, err := Start(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer stop2()

	db, err := Connect(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}
