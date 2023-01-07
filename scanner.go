package slowsync

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/facebookincubator/go-belt/tool/logger/implementation/zap"
	"github.com/xaionaro-go/errors"
	"github.com/xaionaro-go/slowsync/pkg/osrecovery"
)

type dirScanner struct {
	fileTree *fileTree
	rootPath string
	maxDepth uint
}

func newDirScanner(ft *fileTree, rootPath string, maxDepth uint) *dirScanner {
	return &dirScanner{
		fileTree: ft,
		rootPath: rootPath,
		maxDepth: maxDepth,
	}
}

func (s *dirScanner) Start() {
	s.startScanning()
}

func (s *dirScanner) startScanning() {
	s.fileTree.scanWg.Add(1)
	go func() {
		defer s.fileTree.scanWg.Done()
		err := s.scanRootDir()
		if err != nil {
			s.fileTree.addBrokenFile(s.fileTree.rootPath, err)
		}
	}()
}

func (s *dirScanner) scanRootDir() error {
	ctx := context.Background()
	{
		l := zap.Default()
		ctx = logger.CtxWithLogger(ctx, l)
	}

	s.fileTree.semaphore.Acquire(ctx, 1)
	defer s.fileTree.semaphore.Release(1)

	log.Println("scanning dir", s.rootPath, "with maxDepth", s.maxDepth)
	defer log.Println("/scanning dir", s.rootPath, "with maxDepth", s.maxDepth)

	nameCh, errCh, err := osrecovery.List(ctx, s.rootPath)
	if err != nil {
		return errors.New(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errCh {
			err = fmt.Errorf("got error in '%s': %w", s.rootPath, err)
			log.Println(err)
			s.fileTree.addBrokenFile(s.rootPath, err)
		}
	}()

	for fileName := range nameCh {
		if fileName == "." || fileName == ".." {
			continue
		}
		filePath := filepath.Join(s.rootPath, fileName)
		fileInfo, err := os.Lstat(filePath)
		//log.Println("fileInfo:", filePath, fileInfo)
		if err != nil {
			if added, _ := s.fileTree.addBrokenFile(filePath, err); !added {
				// this path was already marked, thus we got into a loop, breaking it
				s.fileTree.addBrokenFile(s.rootPath, err)
				log.Println("got into a loop (case #0):", err)
				return nil
			}
			continue
		}

		if fileInfo.IsDir() {
			if s.maxDepth == 1 {
				continue
			}
			nextDepth := s.maxDepth
			if nextDepth != 0 {
				nextDepth--
			}

			s := newDirScanner(s.fileTree, filePath, nextDepth)
			s.Start()
			continue
		}

		pathRel, err := filepath.Rel(s.fileTree.rootPath, filePath)
		if err != nil {
			return errors.New(err)
		}
		s.fileTree.nodeMapMutex.Lock()
		_, alreadySet := s.fileTree.nodeMap[pathRel]
		s.fileTree.nodeMapMutex.Unlock()
		if alreadySet {
			s.fileTree.addBrokenFile(s.rootPath, fmt.Errorf("got into a cycle getdents in '%s'", s.rootPath))
			log.Println("got into a loop (case #1):", s.rootPath, pathRel)
			continue
		}
		s.fileTree.addNode(node{path: pathRel, size: fileInfo.Size()})
	}
	wg.Wait()

	return nil
}
