// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvtests"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()

	dbDir := filepath.Join(t.TempDir(), "database")
	t.Log("using database dir", dbDir)

	pg, err := New(ctx, dbDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	db := kv.DatabaseFrom(pg)
	if db == nil {
		t.Fatal("failed to open database")
	}

	kvtests.TestEmptyKeyInvalid(ctx, t, db)
	kvtests.TestNonExistentKey(ctx, t, db)
	kvtests.TestNilValueInvalid(ctx, t, db)
	kvtests.TestCommitAfterRollbackIgnored(ctx, t, db)
	kvtests.TestRollbackAfterCommitIgnored(ctx, t, db)
	// kvtests.TestSnapshotIsolation(ctx, t, db)
	kvtests.TestSnapshotRepeatableRead(ctx, t, db)
	kvtests.TestSnapshotFrozenAtCreation(ctx, t, db)
	kvtests.TestDisjointTransactionCommit(ctx, t, db)
	kvtests.TestConflictingTransactionCommit(ctx, t, db)
	kvtests.TestRangeBeginEndInvalid(ctx, t, db)
	kvtests.TestRangeFullDatabaseScan(ctx, t, db)
	kvtests.TestRangeBoundsInclusion(ctx, t, db)
	kvtests.TestRangeDescendBounds(ctx, t, db)
	kvtests.TestSnapshotIteratorStability(ctx, t, db)
	kvtests.TestSnapshotIteratorPrefixRange(ctx, t, db)
	kvtests.TestDiscardedSnapshotBehavior(ctx, t, db)
	kvtests.TestTransactionVisibility(ctx, t, db)
	kvtests.TestTransactionDeleteVisibility(ctx, t, db)
	kvtests.TestTransactionDeleteRecreate(ctx, t, db)
	kvtests.TestTransactionRollbackVisibility(ctx, t, db)
	kvtests.TestLargeValueRoundtrip(ctx, t, db)
	kvtests.TestZeroLengthValue(ctx, t, db)
	kvtests.TestPrefixCleanupTrailingFF(ctx, t, db)
}
