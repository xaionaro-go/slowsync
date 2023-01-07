package main

import (
	"crypto/sha256"
	"database/sql"
	"flag"
	"fmt"
	"hash"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/andy2046/maths"
	"github.com/hashicorp/go-multierror"
	"github.com/xaionaro-go/slowsync"
)

func usage() {
	fmt.Println("hashtree [options] <dir>")
	os.Exit(int(syscall.EINVAL))
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	panic(err)
}

var _ hash.Hash = (*precalculatedHasher)(nil)
var _ slowsync.PrecalculatedDigester = (*precalculatedHasher)(nil)

type precalculatedHasher struct {
	hash.Hash
	digestsMapDBLocker *sync.Mutex
	digestsMapDB       *sql.DB
}

func (h *precalculatedHasher) PrecalculatedDigest(filePath string) []byte {
	h.digestsMapDBLocker.Lock()
	defer h.digestsMapDBLocker.Unlock()
	rows, err := h.digestsMapDB.Query("SELECT digest FROM hashes WHERE path = ?", filePath)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil
	}

	if err := rows.Err(); err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	var digest []byte
	if err := rows.Scan(&digest); err != nil {
		panic(err)
	}

	return digest
}

func newHasherFactory(digestsMapDB *sql.DB) func() hash.Hash {
	if digestsMapDB == nil {
		return func() hash.Hash {
			return sha256.New()
		}
	}

	locker := &sync.Mutex{}
	return func() hash.Hash {
		return &precalculatedHasher{
			Hash:               sha256.New(),
			digestsMapDB:       digestsMapDB,
			digestsMapDBLocker: locker,
		}
	}
}

func main() {
	precalculatedDigestsFilePtr := flag.String("precalculated-digests-file", "", "to avoid rehashing file content by reusing results of 'find <dir> -type f -exec sha256sum {} +'")
	precalculatedDigestsParsedFilePtr := flag.String("precalculated-digests-parsed-dir", "", "reuse parsed 'find <dir> -type f -exec sha256sum {} +' sqlite database")
	sqlite3PathPtr := flag.String("sqlite3db", "", "enables storing the hash tree into an sqlite3 DB")
	netPProfPtr := flag.String("net-pprof", "", "")
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	if *netPProfPtr != "" {
		go func() {
			log.Println(http.ListenAndServe(*netPProfPtr, nil))
		}()
	}

	dir := args[0]
	precalculatedDigestsFile := *precalculatedDigestsFilePtr
	precalculatedDigestsParsedFile := *precalculatedDigestsParsedFilePtr

	if precalculatedDigestsFile != "" && precalculatedDigestsParsedFile != "" {
		panic("cannot set both '-precalculated-digests-file' and '-precalculated-digests-parsed-dir'")
	}

	var digestsMapDB *sql.DB
	if precalculatedDigestsFile != "" {
		var err error
		digestsMapDB, err = parseDigestsFile(precalculatedDigestsFile)
		if digestsMapDB == nil && err != nil {
			panic(err)
		}
		if err != nil {
			for _, err := range err.(*multierror.Error).Errors {
				log.Printf("precalculated digests file error: %v", err)
			}
		}
	}
	if precalculatedDigestsParsedFile != "" {
		var err error
		digestsMapDB, err = openParsedDigestsFile(precalculatedDigestsParsedFile)
		if err != nil {
			panic(err)
		}
	}

	var dbEnabled bool
	var dbTx *sql.Tx
	var dbMutex sync.Mutex
	var dbCommitBegin func()
	if *sqlite3PathPtr != "" {
		dbEnabled = true

		db, err := sql.Open("sqlite3", "file:"+*sqlite3PathPtr+"?cache=shared")
		panicIfError(err)
		db.SetMaxOpenConns(1)

		_, err = db.Exec(`CREATE TABLE hash_tree (path varchar(4096), digest varchar(512), size bigint, mtime bigint, ctime bigint, atime bigint)`)
		panicIfError(err)
		_, err = db.Exec(`CREATE UNIQUE INDEX hash_tree_path_idx ON hash_tree (path)`)
		panicIfError(err)
		_, err = db.Exec(`CREATE INDEX hash_tree_digest_idx ON hash_tree (digest)`)
		panicIfError(err)
		_, err = db.Exec(`CREATE INDEX hash_tree_size_idx ON hash_tree (size)`)
		panicIfError(err)
		_, err = db.Exec(`CREATE INDEX hash_tree_mtime_idx ON hash_tree (mtime)`)
		panicIfError(err)
		_, err = db.Exec(`CREATE INDEX hash_tree_ctime_idx ON hash_tree (ctime)`)
		panicIfError(err)
		_, err = db.Exec(`CREATE INDEX hash_tree_atime_idx ON hash_tree (atime)`)
		panicIfError(err)

		dbCommitBegin = func() {
			dbMutex.Lock()
			defer dbMutex.Unlock()
			dbTx.Commit()
			dbTx, err = db.Begin()
			panicIfError(err)
		}

		dbTx, err = db.Begin()
		panicIfError(err)
		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				dbCommitBegin()
			}
		}()
	}

	limits := slowsync.SetRLimits(1024*1024, 1024*1024*10)
	log.Printf("RLimits: %#+v", limits)
	debug.SetMaxThreads(int(limits.Cur) * 10)

	fileTree, err := slowsync.GetFileTreeWrapper(dir, "", "", 0, maths.Uint64Var.Min(limits.Cur/uint64(len(os.Args))-480, 5000))
	panicIfError(err)

	for item := range fileTree.HashTree(newHasherFactory(digestsMapDB)) {
		filePath := path.Clean(item.Path)
		fmt.Printf("%s\n", item.String())
		if item.Error == nil && dbEnabled {
			dbMutex.Lock()
			_, err := dbTx.Exec(`INSERT INTO hash_tree (path, digest, size, mtime, ctime, atime) VALUES (?, ?, ?, ?, ?, ?)`,
				filePath, item.Digest, item.Size, item.ModifyTime.UnixNano(), item.ChangeTime.UnixNano(), item.AccessTime.UnixNano())
			dbMutex.Unlock()
			panicIfError(err)
		}
	}

	if dbEnabled {
		dbCommitBegin()
	}

	log.Println("end")
}
