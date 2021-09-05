package slowsync

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

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

	cachePath string
	cacheDB   *sql.DB

	brokenFilesList     *os.File
	brokenFilesListPath string
	brokenFilesMap      map[string]bool
	brokenFilesMapMutex sync.Mutex
}

type FileTree interface {
	SyncTo(FileTree, bool) error
	SetBrokenFilesList(path string) error
}

func GetFileTree(dir string, maxOpenFiles uint64) (FileTree, error) {
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
	err = ft.scan()
	if err != nil {
		return nil, errors.New(err)
	}
	return ft, nil
}

func GetCachedFileTree(dir, cachePath string, maxOpenFiles uint64) (FileTree, error) {
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
		err := ft.scan()
		if err != nil {
			return nil, errors.New(err)
		}
		go func() {
			ft.scanWg.Wait()
			close(ft.nodeChan)
		}()
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
		ft.nodeMap[node.path] = node
	}

	err = rows.Err()
	if err != nil {
		return errors.New(err)
	}
	return nil
}

func (ft *fileTree) addNode(path string, node node) {
	ft.nodeMapMutex.Lock()
	ft.nodeMap[path] = node
	ft.nodeMapMutex.Unlock()
	ft.nodeChan <- node
	if ft.cacheDB != nil {
		ft.cacheDB.Exec(`INSERT INTO file_tree (path, size) VALUES (?, ?)`, path, node.size)
	}
}

func (ft *fileTree) scan() error {
	log.Println("Scanning", ft.rootPath)
	go func() {
		ft.scanWg.Wait()
		log.Println("Scanning", ft.rootPath, "-- complete")
	}()

	return ft.scanDir(ft.rootPath)
}

func (ft *fileTree) scanDir(rootPath string) error {
	ft.scanWg.Add(1)
	defer ft.scanWg.Done()

	ft.semaphore.Acquire(context.TODO(), 1)
	defer ft.semaphore.Release(1)

	if ft.cacheDB != nil {
		ft.cacheDB.Exec(`START TRANSACTION`)
		defer ft.cacheDB.Exec(`COMMIT`)
	}
	f, err := os.Open(rootPath)
	if err != nil {
		return errors.New(err)
	}
	defer f.Close()
	for {
		list, err := f.Readdir(1)
		if err != nil {
			if err != io.EOF {
				ft.addBrokenFile(rootPath, err)
			}
			break
		}

		for _, fileInfo := range list {
			if fileInfo.IsDir() {
				ft.scanWg.Add(1)
				go func(fileInfo os.FileInfo) {
					defer ft.scanWg.Done()
					filePath := filepath.Join(rootPath, fileInfo.Name())
					err := ft.scanDir(filePath)
					if err != nil {
						ft.addBrokenFile(filePath, err)
					}
				}(fileInfo)
				continue
			}

			pathRel, err := filepath.Rel(ft.rootPath, filepath.Join(rootPath, fileInfo.Name()))
			if err != nil {
				return errors.New(err)
			}
			ft.addNode(pathRel, node{size: fileInfo.Size()})
		}
	}

	return nil
}
func (src *fileTree) SyncTo(dstI FileTree, dryRun bool) error {
	log.Println("Syncing: wait for SRC to complete scanning")
	defer log.Println("Syncing -- complete")

	var filesToCopy []string

	dst := dstI.(*fileTree)
	for range dst.nodeChan {
	}

	log.Println("Syncing: filtering")

	for srcNode := range src.nodeChan {
		if src.brokenFilesMap != nil {
			if src.brokenFilesMap[srcNode.path] {
				continue
			}
		}

		dstNode := dst.nodeMap[srcNode.path]
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

		go func(filePath string) {
			src.semaphore.Acquire(context.TODO(), 2)
			defer src.semaphore.Release(2)
			dstDir := filepath.Dir(path.Join(dst.rootPath, filePath))
			err := createDirectory(dstDir)
			if err != nil {
				fmt.Println("cannot create directory", dstDir)
				return
			}
			err = copyFileContents(path.Join(src.rootPath, filePath), path.Join(dst.rootPath, filePath))
			if err != nil {
				// TODO: consider possible errors on the destination side
				err = src.addBrokenFile(filePath, err)
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
	for scanner.Scan() {
		ft.brokenFilesMap[scanner.Text()] = true
	}

	if err := scanner.Err(); err != nil {
		return errors.New(err)
	}

	return nil
}

func (src *fileTree) addBrokenFile(filePath string, fileErr error) error {
	fmt.Println("broken file:", filePath, fileErr)
	if src.brokenFilesList == nil {
		return nil
	}
	src.brokenFilesMapMutex.Lock()
	defer src.brokenFilesMapMutex.Unlock()
	src.brokenFilesMap[filePath] = true
	_, err := src.brokenFilesList.Write([]byte(fmt.Sprintf("%s\n", filePath)))
	return errors.Wrap(err)
}

func createDirectory(dir string) error {
	return os.MkdirAll(dir, os.ModePerm)
}

func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
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
