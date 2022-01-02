package slowsync

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xaionaro-go/errors"
	"golang.org/x/sync/semaphore"
)

type node struct {
	path string
	size int64
}

type fileTree struct {
	rootPath string

	nodeChan     chan node
	nodeMap      map[string]node
	nodeMapMutex sync.Mutex
	semaphore    *semaphore.Weighted

	scanWg sync.WaitGroup

	cachePath       string
	cacheDB         *sql.DB
	cacheDBTX       *sql.Tx
	cacheDBTXLocker sync.Mutex

	brokenFilesList     *os.File
	brokenFilesListPath string
	brokenFilesMap      map[string]bool
	brokenFilesMapMutex sync.Mutex
}

type FileTree interface {
	SyncTo(FileTree, []FileTree, bool) error
	HashTree(func() hash.Hash) chan HashTreeItem
	SetBrokenFilesList(path string) error
	SplitList(hasher hash.Hash, levels uint, perm os.FileMode, skipChars uint) error
}

func GetFileTree(dir string, maxDepth uint, maxOpenFiles uint64) (FileTree, error) {
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	ft := &fileTree{
		rootPath:       dir,
		nodeChan:       make(chan node, 1024),
		nodeMap:        map[string]node{},
		brokenFilesMap: map[string]bool{},
		semaphore:      semaphore.NewWeighted(int64(maxOpenFiles)),
	}
	ft.backgroundScan(maxDepth)
	return ft, nil
}

type PrecalculatedDigester interface {
	PrecalculatedDigest(filePath string) []byte
}

func (ft *fileTree) HashTree(
	hasherFactory func() hash.Hash,
) chan HashTreeItem {
	result := make(chan HashTreeItem)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 1024; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				hasher := hasherFactory()
				for srcNode := range ft.nodeChan {
					path := filepath.Join(ft.rootPath, srcNode.path)
					s, err := os.Stat(path)
					if err != nil {
						result <- HashTreeItem{
							Path:  srcNode.path,
							Error: fmt.Errorf("unable to stat(): %w", err),
						}
						continue
					}

					if !s.Mode().IsRegular() {
						continue
					}

					var digest []byte
					if digestGetter, ok := hasher.(PrecalculatedDigester); ok {
						digest = digestGetter.PrecalculatedDigest(srcNode.path)
						if digest == nil {
							log.Printf("no precalculated digest for '%s' ('%s')", path, srcNode.path)
						}
					}

					if digest == nil {
						f, err := os.Open(path)
						if err != nil {
							result <- HashTreeItem{
								Path:  srcNode.path,
								Error: fmt.Errorf("unable to open(): %w", err),
							}
							continue
						}
						io.Copy(hasher, f)
						f.Close()
						digest = hasher.Sum(nil)
					}

					item := HashTreeItem{
						Path:       srcNode.path,
						Digest:     digest,
						Size:       uint64(s.Size()),
						ModifyTime: s.ModTime(),
					}
					if us, ok := s.Sys().(*syscall.Stat_t); ok {
						item.ModifyTime = time.Unix(int64(us.Mtim.Sec), int64(us.Ctim.Nsec))
						item.ChangeTime = time.Unix(int64(us.Ctim.Sec), int64(us.Mtim.Nsec))
						item.AccessTime = time.Unix(int64(us.Atim.Sec), int64(us.Atim.Nsec))
					}

					result <- item
				}
			}()
		}

		wg.Wait()
		close(result)
	}()
	return result
}

func GetCachedFileTree(dir, cachePath string, maxDepth uint, maxOpenFiles uint64) (FileTree, error) {
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	ft := &fileTree{
		rootPath:       dir,
		cachePath:      cachePath,
		nodeChan:       make(chan node, 1024),
		nodeMap:        map[string]node{},
		brokenFilesMap: map[string]bool{},
		semaphore:      semaphore.NewWeighted(int64(maxOpenFiles)),
	}

	hasCache := true
	if _, err := os.Stat(ft.cachePath); os.IsNotExist(err) {
		hasCache = false
	}

	ft.cacheDB, err = sql.Open("sqlite3", "file:"+ft.cachePath+"?cache=shared")
	if err != nil {
		return nil, errors.New(err)
	}
	ft.cacheDB.SetMaxOpenConns(1)

	if hasCache {
		go func() {
			log.Println("Reading the cache from", ft.cachePath)
			err = ft.readCache()
			log.Println("Reading the cache from", ft.cachePath, "-- complete")
			close(ft.nodeChan)
		}()
	} else {
		_, err = ft.cacheDB.Exec(`CREATE TABLE file_tree (path varchar(4096), size bigint)`)
		if err != nil {
			return nil, errors.New(err)
		}
		_, err = ft.cacheDB.Exec(`CREATE UNIQUE INDEX file_tree_idx_path ON file_tree (path)`)
		if err != nil {
			return nil, errors.New(err)
		}
		ft.backgroundScan(maxDepth)
	}
	if err != nil {
		return nil, errors.New(err)
	}

	return ft, nil
}

func (ft *fileTree) readCache() error {
	rows, err := ft.cacheDB.Query("SELECT path, size FROM file_tree")
	if err != nil {
		return errors.New(err)
	}

	defer rows.Close()

	for rows.Next() {
		var node node
		rows.Scan(&node.path, &node.size)

		ft.nodeChan <- node
		ft.nodeMapMutex.Lock()
		ft.nodeMap[node.path] = node
		ft.nodeMapMutex.Unlock()
	}

	err = rows.Err()
	if err != nil {
		return errors.New(err)
	}
	return nil
}

func (ft *fileTree) SplitList(hasher hash.Hash, levels uint, perm os.FileMode, skipChars uint) error {
	if hasher.Size() > 0 {
		if levels > uint(hasher.Size()) {
			return fmt.Errorf("too many levels: %d > %d", levels, uint(hasher.Size()))
		}
	}
	for srcNode := range ft.nodeChan {
		srcPath := filepath.Join(ft.rootPath, srcNode.path)
		s, err := os.Stat(srcPath)
		if err != nil {
			return fmt.Errorf("unable to stat(): %w", err)
		}

		if s.Mode().IsDir() {
			continue
		}

		baseName := filepath.Base(srcPath)
		if _, err = hasher.Write([]byte(baseName)); err != nil {
			return fmt.Errorf("unable to hash '%s': %w", baseName, err)
		}
		var hashedName string
		if stringer, ok := hasher.(fmt.Stringer); ok {
			hashedName = stringer.String()
		} else {
			hashedName = hex.EncodeToString(hasher.Sum(nil))
		}
		hasher.Reset()
		maxLevels := levels
		if uint(len(hashedName)) < maxLevels {
			maxLevels = uint(len(hashedName))
		}
		dirPath := ft.rootPath
		for level := skipChars; level < maxLevels; level++ {
			dirName := hashedName[level : level+1]
			dirPath = filepath.Join(dirPath, dirName)
		}

		if err := os.MkdirAll(dirPath, perm); err != nil {
			return fmt.Errorf("unable to make dir '%s': %w", dirPath, err)
		}
		dstPath := filepath.Join(dirPath, baseName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return fmt.Errorf("unable to rename '%s' to '%s': %w", srcPath, dstPath, err)
		}
	}
	return nil
}

func (ft *fileTree) addNode(node node) {
	ft.nodeMapMutex.Lock()
	ft.nodeMap[node.path] = node
	ft.nodeMapMutex.Unlock()
	ft.nodeChan <- node

	ft.cacheDBTXLocker.Lock()
	defer ft.cacheDBTXLocker.Unlock()
	if ft.cacheDBTX != nil {
		ft.cacheDBTX.Exec(`INSERT INTO file_tree (path, size) VALUES (?, ?)`, node.path, node.size)
	}
}

func (ft *fileTree) backgroundScan(maxDepth uint) {
	log.Println("Scanning", ft.rootPath)

	ctx, cancelFn := context.WithCancel(context.Background())
	if ft.cacheDB != nil {
		ft.commitCacheDBTX() // to create the first transaction

		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()

			// to commit the last transaction
			defer func() {
				ft.commitCacheDBTX()
				ft.cacheDBTX = nil
			}()

			for {
				select {
				case <-ticker.C:
					ft.commitCacheDBTX()
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	ft.scanWg.Add(1)
	go func() {
		defer ft.scanWg.Done()
		err := ft.scanDir(ft.rootPath, maxDepth)
		if err != nil {
			ft.addBrokenFile(ft.rootPath, err)
		}
	}()

	go func() {
		ft.scanWg.Wait()
		cancelFn()
		close(ft.nodeChan)
		log.Println("Scanning", ft.rootPath, "-- complete")
	}()
}

func (ft *fileTree) commitCacheDBTX() {
	log.Println("committing cache DB changes")
	defer log.Println("committed cache DB changes")

	ft.cacheDBTXLocker.Lock()
	defer ft.cacheDBTXLocker.Unlock()

	if ft.cacheDBTX != nil {
		ft.cacheDBTX.Commit()
	}

	var err error
	ft.cacheDBTX, err = ft.cacheDB.Begin()
	if err != nil {
		panic(err)
	}
}

func (ft *fileTree) scanDir(rootPath string, maxDepth uint) error {
	ft.semaphore.Acquire(context.TODO(), 1)
	defer ft.semaphore.Release(1)

	f, err := os.Open(rootPath)
	if err != nil {
		return errors.New(err)
	}
	defer f.Close()
	for {
		names, err := f.Readdirnames(1)
		if err != nil {
			if err != io.EOF {
				ft.addBrokenFile(rootPath, err)
			}
			break
		}

		for _, fileName := range names {
			filePath := filepath.Join(rootPath, fileName)
			fileInfo, err := os.Lstat(filePath)
			if err != nil {
				ft.addBrokenFile(filePath, err)
				continue
			}

			if fileInfo.IsDir() {
				if maxDepth != 1 {
					ft.scanWg.Add(1)
					go func(filePath string, fileInfo os.FileInfo) {
						defer ft.scanWg.Done()

						nextDepth := maxDepth
						if nextDepth != 0 {
							nextDepth--
						}
						err := ft.scanDir(filePath, nextDepth)
						if err != nil {
							ft.addBrokenFile(filePath, err)
						}
					}(filePath, fileInfo)
				}
				continue
			}

			pathRel, err := filepath.Rel(ft.rootPath, filePath)
			if err != nil {
				return errors.New(err)
			}
			ft.nodeMapMutex.Lock()
			_, alreadySet := ft.nodeMap[pathRel]
			ft.nodeMapMutex.Unlock()
			if alreadySet {
				ft.addBrokenFile(rootPath, fmt.Errorf("got into a cycle getdents in '%s'", rootPath))
				return nil
			}
			ft.addNode(node{path: pathRel, size: fileInfo.Size()})
		}
	}

	return nil
}

func (ft *fileTree) SyncTo(
	dstI FileTree,
	excludeFTs []FileTree,
	dryRun bool,
) error {
	return ft.syncTo(dstI.(*fileTree).rootPath, dstI, excludeFTs, dryRun)
}

func (ft *fileTree) syncTo(
	dstRootDir string,
	cmpI FileTree,
	excludeFTIs []FileTree,
	dryRun bool,
) error {
	log.Println("Syncing: wait for DST and EXC to complete scanning")
	defer log.Println("Syncing -- complete")

	var excludeFTs []*fileTree
	for _, ft := range excludeFTIs {
		excludeFTs = append(excludeFTs, ft.(*fileTree))
	}

	var filesToCopy []string

	cmp := cmpI.(*fileTree)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range cmp.nodeChan {
		}
	}()

	for _, excFT := range excludeFTs {
		wg.Add(1)
		go func(excFT *fileTree) {
			defer wg.Done()
			for range excFT.nodeChan {
			}
		}(excFT)
	}

	wg.Wait()

	log.Println("Syncing: filtering")

	for srcNode := range ft.nodeChan {
		if ft.brokenFilesMap != nil {
			ft.brokenFilesMapMutex.Lock()
			isBrokenFile := ft.brokenFilesMap[srcNode.path]
			ft.brokenFilesMapMutex.Unlock()
			if isBrokenFile {
				continue
			}
		}

		dstNode := cmp.nodeMap[srcNode.path]
		if srcNode.size == dstNode.size {
			continue
		}
		filesToCopy = append(filesToCopy, srcNode.path)
	}

	sort.Strings(filesToCopy)

	fmt.Println("Syncing: to copy report")
	for _, filePath := range filesToCopy {
		fmt.Println(filePath)
	}
	fmt.Println("Syncing: to copy report -- complete")

	log.Println("Syncing: copying")

	for _, filePath := range filesToCopy {
		if dryRun {
			continue
		}

		shouldExclude := false
		for _, excFT := range excludeFTs {
			_, ok := excFT.nodeMap[filePath]
			if ok {
				shouldExclude = true
				break
			}
		}
		if shouldExclude {
			continue
		}

		go func(filePath string) {
			ft.semaphore.Acquire(context.TODO(), 2)
			defer ft.semaphore.Release(2)
			dstDir := filepath.Dir(path.Join(dstRootDir, filePath))
			err := createDirectory(dstDir)
			if err != nil {
				fmt.Println("cannot create directory", dstDir)
				return
			}
			err = copyFileContents(path.Join(ft.rootPath, filePath), path.Join(dstRootDir, filePath))
			if err != nil {
				// TODO: consider possible errors on the destination side
				err = ft.addBrokenFile(filePath, err)
				if err != nil {
					panic(err)
				}
			}
		}(filePath)
	}

	return nil
}

func (ft *fileTree) SetBrokenFilesList(path string) error {
	var err error

	if ft.brokenFilesList != nil {
		ft.brokenFilesList.Close()
	}

	ft.brokenFilesListPath = path

	if _, err := os.Stat(ft.brokenFilesListPath); os.IsNotExist(err) {
		ft.brokenFilesList, err = os.Create(ft.brokenFilesListPath)
		return errors.Wrap(err, ft.brokenFilesListPath)
	}

	ft.brokenFilesList, err = os.OpenFile(ft.brokenFilesListPath, os.O_APPEND|os.O_RDWR, os.ModeAppend)
	if err != nil {
		return errors.New(err, ft.brokenFilesListPath)
	}

	scanner := bufio.NewScanner(ft.brokenFilesList)
	ft.brokenFilesMapMutex.Lock()
	for scanner.Scan() {
		ft.brokenFilesMap[scanner.Text()] = true
	}
	defer ft.brokenFilesMapMutex.Unlock()

	if err := scanner.Err(); err != nil {
		return errors.New(err)
	}

	return nil
}

func (ft *fileTree) addBrokenFile(filePath string, fileErr error) error {
	fmt.Println("broken file:", filePath, fileErr)
	if ft.brokenFilesList == nil {
		return nil
	}
	ft.brokenFilesMapMutex.Lock()
	defer ft.brokenFilesMapMutex.Unlock()
	ft.brokenFilesMap[filePath] = true
	_, err := ft.brokenFilesList.Write([]byte(fmt.Sprintf("%s\n", filePath)))
	return errors.Wrap(err)
}

func createDirectory(dir string) error {
	return os.MkdirAll(dir, os.ModePerm)
}

func copyFileContents(ft, dst string) (err error) {
	in, err := os.Open(ft)
	if err != nil {
		return errors.New(err)
	}

	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return errors.New(err)
	}

	defer func() {
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
	}()

	buf := make([]byte, 1024*1024)

	writeResultChan := make(chan error, 1)
	writeResultChan <- nil

	for {
		rn, err := io.ReadFull(in, buf)
		wErr := <-writeResultChan
		if wErr != nil {
			return errors.Wrap(wErr)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err)
		}

		go func(rn int) {
			wn, err := out.Write(buf)
			if wn != rn {
				writeResultChan <- fmt.Errorf("written != read: %d != %d", wn, rn)
				return
			}
			if err != nil {
				writeResultChan <- err
				return
			}
			writeResultChan <- nil
		}(rn)
	}

	/*if _, err = io.Copy(out, in); err != nil {
		return errors.New(err)
	}*/
	return
}
