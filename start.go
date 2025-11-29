// Copyright (c) 2025 Visvasity LLC

package kvpostgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/lib/pq"
)

// PgctlBinaryPath variable contains the path to `pg_ctl` binary. When empty
// exec.LookPath will be used to find the binary.
var PgctlBinaryPath = ""

type pgCtl struct {
	binPath string
}

func (v *pgCtl) init(ctx context.Context, dataDir string) (status error) {
	tmpDir, err := os.MkdirTemp(filepath.Dir(dataDir), ".pgdir")
	if err != nil {
		return err
	}
	defer func() {
		if status != nil {
			os.RemoveAll(tmpDir)
		}
	}()

	cmd := exec.CommandContext(ctx, v.binPath, "initdb",
		"-D", tmpDir, // Data directory.
		"-o", "--auth-host=reject", // Do not allow TCP connections
		"-o", "--auth-local=trust", // Trust all local users over unix domain sockets
		"-o", "-U postgres", // Key-Value database uses postgres as the username
		"-o", "-c listen_addresses=''", // Disable listening for TCP connections
		"-o", "-c unix_socket_directories="+dataDir, // Unix domain socket is place in the same pg data directory
		"-o", "-c log_min_messages=INFO", // INFO level.
		"-o", "-c logging_collector=on", // Save logs to files in a directory
	)
	slog.Info("initializing the postgres database", "cmd", cmd.Args)

	// cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		slog.Warn("could not initialize postgres database", "err", err)
		return err
	}
	if err := os.Rename(tmpDir, dataDir); err != nil {
		return err
	}
	if _, err := os.Stat(dataDir); err != nil {
		return err
	}
	slog.Info("database directory is initialized successfully", "dir", dataDir)
	return nil
}

func (v *pgCtl) running(ctx context.Context, dataDir string) (status error) {
	cmd := exec.CommandContext(ctx, v.binPath, "-D", dataDir, "status")
	slog.Info("checking if postgres database is running", "cmd", cmd.Args)
	if out, err := cmd.CombinedOutput(); err != nil {
		code := cmd.ProcessState.ExitCode()
		if code == 3 {
			return os.ErrNotExist
		}
		slog.Warn("pg_ctl status command failed", "exit-code", code, "output", string(bytes.TrimSpace(out)))
		return fmt.Errorf("pg_ctl status cmd failed with code %d: %w", code, os.ErrInvalid)
	}
	return nil
}

func (v *pgCtl) start(ctx context.Context, dataDir string) (status error) {
	var cmd *exec.Cmd
	if _, err := os.Stat(filepath.Join(dataDir, "postmaster.opts")); err == nil {
		cmd = exec.CommandContext(ctx, v.binPath, "restart", "-D", dataDir, "--wait")
	} else {
		cmd = exec.CommandContext(ctx, v.binPath, "start", "-D", dataDir, "--wait")
	}
	slog.Info("starting the postgres database", "cmd", cmd.Args)

	// cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		slog.Warn("could not start/restart postgres database", "cmd", cmd.Args, "err", err)
		return err
	}
	defer func() {
		if status != nil {
			v.stop(dataDir)
		}
	}()

	// Create the 'default' database if it doesn't already exist. We connect to
	// the database without a target database name in the connection-string.

	cs := fmt.Sprintf("user=postgres host=%s", dataDir)
	connector, err := pq.NewConnector(cs)
	if err != nil {
		return err
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	q := `SELECT FROM pg_database WHERE datname = $1`
	row := db.QueryRowContext(ctx, q, defaultDB)
	switch err := row.Scan(); {
	case err == sql.ErrNoRows:
		slog.Info("creating new database because default database does not exist", "database", defaultDB)
		if _, err := db.ExecContext(ctx, `CREATE DATABASE `+defaultDB); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		slog.Info("default database already exists", "database", defaultDB)
	}
	return nil
}

func (v *pgCtl) stop(dataDir string) error {
	cmd := exec.Command(v.binPath, "stop", "-D", dataDir, "--wait")
	slog.Info("stopping the postgres database", "cmd", cmd.Args)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// Start initializes a postgres database and starts a private postgres server
// in the given directory if it doesn't already exist.
func Start(ctx context.Context, dataDir string) (func(), error) {
	if !filepath.IsAbs(dataDir) {
		absDir, err := filepath.Abs(dataDir)
		if err != nil {
			return nil, err
		}
		dataDir = absDir
	}

	pgctl := PgctlBinaryPath
	if len(pgctl) == 0 {
		binPath, err := exec.LookPath("pg_ctl")
		if err != nil {
			return nil, err
		}
		pgctl = binPath
	}
	if !filepath.IsAbs(pgctl) {
		binPath, err := filepath.Abs(pgctl)
		if err != nil {
			return nil, err
		}
		pgctl = binPath
	}
	if _, err := os.Stat(pgctl); err != nil {
		return nil, err
	}

	v := &pgCtl{
		binPath: pgctl,
	}

	if _, err := os.Stat(dataDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err := v.init(ctx, dataDir); err != nil {
			return nil, err
		}
	}

	if err := v.running(ctx, dataDir); err == nil {
		return func() {}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err := v.start(ctx, dataDir); err != nil {
		return nil, err
	}
	return func() { v.stop(dataDir) }, nil
}
