package main

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/hashicorp/go-multierror"
)

func openParsedDigestsFile(parsedDigestsDirPath string) (result *sql.DB, err error) {
	cachePath := filepath.Join(parsedDigestsDirPath, "db")

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", cachePath))
	if err != nil {
		return nil, fmt.Errorf("unable to open file a file '%s' as an SQlite3 DB: %w", cachePath, err)
	}
	db.SetMaxOpenConns(1)

	return db, nil
}

func parseDigestsFile(digestsFilePath string) (result *sql.DB, err error) {
	var itemsErr error

	tempDir, err := os.MkdirTemp(filepath.Dir(digestsFilePath), "parsedDigestsFile-")
	if err != nil {
		return nil, fmt.Errorf("unable to create temporary directory: %w", err)
	}

	cachePath := filepath.Join(tempDir, "db")
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", cachePath))
	if err != nil {
		return nil, fmt.Errorf("unable to open file a file '%s' as an SQlite3 DB: %w", cachePath, err)
	}
	db.SetMaxOpenConns(1)

	runtime.SetFinalizer(db, func(db *sql.DB) {
		db.Close()
		os.RemoveAll(tempDir)
	})

	if _, err := db.Exec(`CREATE TABLE hashes (path varchar(4096), digest varchar(512))`); err != nil {
		return nil, fmt.Errorf("unable to create table 'hashes' in file '%s': %w", cachePath, err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX hashes_path_idx ON hashes (path)`); err != nil {
		return nil, fmt.Errorf("unable to create an index: %w", err)
	}

	f, err := os.Open(digestsFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open '%s': %w", digestsFilePath, err)
	}
	defer f.Close()

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("unable to start a transaction: %w", err)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "  ", 2)
		digest, err := hex.DecodeString(parts[0])
		if err != nil {
			itemsErr = multierror.Append(itemsErr, fmt.Errorf("unable to unhex digest '%s': %w", parts[1], err))
		}
		filePath := filepath.Clean(parts[1])

		if _, err = tx.Exec(`INSERT INTO hashes (path, digest) VALUES (?, ?)`, filePath, digest); err != nil {
			return nil, fmt.Errorf("unable to save to DB: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan '%s': %w", digestsFilePath, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("unable to commit the transaction: %w", err)
	}

	return db, itemsErr
}
