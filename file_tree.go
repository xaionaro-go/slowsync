package slowsync

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xaionaro-go/errors"
)

type node struct {
	size int64
}

type fileTree struct {
	nodes map[string]node

	rootPath string

	cachePath string
	cacheDB   *sql.DB

	brokenFilesList     *os.File
	brokenFilesListPath string
	brokenFilesMap      map[string]bool
}

type FileTree interface {
	SyncTo(FileTree, bool) error
	SetBrokenFilesList(path string) error
}

func GetFileTree(dir string) (FileTree, error) {
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	ft := &fileTree{
		rootPath: dir,
	}
	err = ft.Scan()
	if err != nil {
		return nil, errors.New(err)
	}
	return ft, nil
}

func GetCachedFileTree(dir, cachePath string) (FileTree, error) {
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	ft := &fileTree{
		rootPath:  dir,
		cachePath: cachePath,
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
		log.Println("Reading the cache from", ft.cachePath)
		err = ft.ReadCache()
		log.Println("Reading the cache from", ft.cachePath, "-- complete")
	} else {
		_, err = ft.cacheDB.Exec(`CREATE TABLE file_tree (path varchar(4096), size bigint)`)
		if err != nil {
			return nil, errors.New(err)
		}
		_, err = ft.cacheDB.Exec(`CREATE UNIQUE INDEX file_tree_idx_path ON file_tree (path)`)
		if err != nil {
			return nil, errors.New(err)
		}
		err = ft.Scan()
	}
	if err != nil {
		return nil, errors.New(err)
	}

	return ft, nil
}

func (ft *fileTree) ReadCache() error {
	rows, err := ft.cacheDB.Query("SELECT path, size FROM file_tree")
	if err != nil {
		return errors.New(err)
	}

	ft.nodes = map[string]node{}

	defer rows.Close()

	for rows.Next() {
		var filePath string
		var fileSize int64
		rows.Scan(&filePath, &fileSize)

		ft.nodes[filePath] = node{
			size: fileSize,
		}
	}

	err = rows.Err()
	if err != nil {
		return errors.New(err)
	}
	return nil
}

func (ft *fileTree) addNode(path string, node node) {
	ft.nodes[path] = node
	if ft.cacheDB != nil {
		ft.cacheDB.Exec(`INSERT INTO file_tree (path, size) VALUES (?, ?)`, path, node.size)
	}
}

func (ft *fileTree) Scan() error {
	log.Println("Scanning", ft.rootPath)
	defer log.Println("Scanning", ft.rootPath, "-- complete")

	if ft.cacheDB != nil {
		ft.cacheDB.Exec(`START TRANSACTION`)
		defer ft.cacheDB.Exec(`COMMIT`)
	}

	ft.nodes = map[string]node{}
	return filepath.Walk(ft.rootPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			err = ft.addBrokenFile(filePath, err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		pathRel, err := filepath.Rel(ft.rootPath, filePath)
		if err != nil {
			return errors.New(err)
		}
		ft.addNode(pathRel, node{size: info.Size()})
		return nil
	})
}
func (src *fileTree) SyncTo(dstI FileTree, dryRun bool) error {
	log.Println("Syncing")
	defer log.Println("Syncing -- complete")

	filesToCopy := []string{}

	dst := dstI.(*fileTree)

	for filePath, srcNode := range src.nodes {
		if src.brokenFilesMap != nil {
			if src.brokenFilesMap[filePath] {
				continue
			}
		}

		dstNode := dst.nodes[filePath]
		if srcNode.size == dstNode.size {
			continue
		}
		filesToCopy = append(filesToCopy, filePath)
	}

	sort.Strings(filesToCopy)

	/*fmt.Println("to copy")
	for _, filePath := range filesToCopy {
		fmt.Println(filePath)
	}
	fmt.Println("to copy -- complete")*/

	onePercentCount := (len(filesToCopy) + 99) / 100

	for idx, filePath := range filesToCopy {
		/*if idx%onePercentCount == 0 {
			fmt.Println(idx/onePercentCount, "%")
		}*/
		fmt.Println(idx/onePercentCount, "%: coping:", filePath)
		if dryRun {
			continue
		}

		dstDir := filepath.Dir(path.Join(dst.rootPath, filePath))
		err := createDirectory(dstDir)
		if err != nil {
			fmt.Println("cannot create directory", dstDir)
			continue
		}
		err = copyFileContents(path.Join(src.rootPath, filePath), path.Join(dst.rootPath, filePath))
		if err != nil {
			// TODO: consider possible errors on the destination side
			err = src.addBrokenFile(filePath, err)
			if err != nil {
				return errors.New(err)
			}
		}
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

	ft.brokenFilesMap = map[string]bool{}

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
	src.brokenFilesMap[filePath] = true
	_, err := src.brokenFilesList.Write([]byte(fmt.Sprintf("%s\n", src)))
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

	writeResultChan := make(chan error)

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

	if _, err = io.Copy(out, in); err != nil {
		return errors.New(err)
	}
	return
}
