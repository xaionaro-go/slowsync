package osrecovery

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/facebookincubator/go-belt/beltctx"
)

var (
	listHandlerSourcePath   string
	listHandlerCompiledPath string
	listHandlerInitOnce     sync.Once

	//go:embed list_linux/main.c
	listHandlerSource string
)

func List(ctx context.Context, path string) (<-chan string, <-chan error, error) {
	lazyInitOpenHandler()

	ctx, listOutputParser := newListOutputParser(beltctx.WithField(ctx, "command", []string{listHandlerCompiledPath, path}))
	cmd := exec.CommandContext(ctx, listHandlerCompiledPath, path)
	cmd.Stdout = listOutputParser
	err := cmd.Start()

	if err != nil {
		return nil, nil, fmt.Errorf(
			"unable to start command '%s %s': %w",
			listHandlerCompiledPath,
			path,
			err,
		)
	}
	go func() {
		cmd.Wait()
		listOutputParser.Close()
	}()
	listOutputParser.StartWatchDog(ctx)
	return listOutputParser.NameCh, listOutputParser.ErrCh, nil
}

func lazyInitOpenHandler() {
	listHandlerInitOnce.Do(func() {
		initOpenHandler()
	})
}

func initOpenHandler() {
	listHandlerDir := filepath.Join(os.TempDir(), "osrecovery-initListHandler")
	listHandlerSourcePath = filepath.Join(listHandlerDir, "main.c")
	listHandlerCompiledPath = filepath.Join(listHandlerDir, "compiled")
	defer func() {
		log.Println("listHandlerPath:", listHandlerCompiledPath)
	}()
	oldSource, err := ioutil.ReadFile(listHandlerSourcePath)
	if err == nil && bytes.Equal(oldSource, []byte(listHandlerSource)) {
		fInfo, err := os.Stat(listHandlerCompiledPath)
		if err == nil && fInfo.Size() > 0 && fInfo.Mode().IsRegular() {
			return
		} else {
			listHandlerDir, err = os.MkdirTemp(os.TempDir(), "osrecovery-initListHandler-")
			if err != nil {
				panic(err)
			}

			listHandlerSourcePath = filepath.Join(listHandlerDir, "main.c")
			listHandlerCompiledPath = filepath.Join(listHandlerDir, "compiled")
		}
	}

	err = os.MkdirAll(listHandlerDir, 0755)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(listHandlerSourcePath, []byte(listHandlerSource), 0640)
	if err != nil {
		panic(err)
	}

	cmd := exec.Command("gcc", "-o", listHandlerCompiledPath, listHandlerSourcePath)
	err = cmd.Run()
	if err != nil {
		panic(err)
	}
}
