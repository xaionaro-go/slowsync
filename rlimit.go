package slowsync

import (
	"log"
	"syscall"

	"github.com/andy2046/maths"
)

func setRLimit(rLimitID int, rlimitValue uint64) syscall.Rlimit {
	var rLimit syscall.Rlimit
	rLimit.Max = rlimitValue
	rLimit.Cur = rlimitValue
	err := syscall.Setrlimit(rLimitID, &rLimit)
	if err != nil {
		log.Println("Error setting rlimit", err)
	}

	err = syscall.Getrlimit(rLimitID, &rLimit)
	if err != nil {
		log.Println("Error getting rlimit", err)
		rLimit.Cur = 1024
	}
	return rLimit
}

const (
	RLIMIT_NPROC = 0x6
)

func SetRLimits(
	noFileLimit uint64,
	nProc uint64,
) syscall.Rlimit {
	rLimitFileNo := setRLimit(syscall.RLIMIT_NOFILE, noFileLimit)
	rLimitNProc := setRLimit(RLIMIT_NPROC, nProc)
	return syscall.Rlimit{
		Cur: maths.Uint64Var.Min(rLimitFileNo.Cur, rLimitNProc.Cur),
		Max: maths.Uint64Var.Min(rLimitFileNo.Max, rLimitNProc.Max),
	}
}

func GetFileTreeWrapper(dir, cachePath, brokenFilesList string, maxDepth uint, maxOpenFiles uint64) (FileTree, error) {
	var fileTree FileTree
	var err error
	if cachePath == "" {
		fileTree, err = GetFileTree(dir, maxDepth, maxOpenFiles)
	} else {
		fileTree, err = GetCachedFileTree(dir, cachePath, maxDepth, maxOpenFiles)
	}
	if err != nil {
		return nil, err
	}
	if brokenFilesList != "" {
		err = fileTree.SetBrokenFilesList(brokenFilesList)
	}
	return fileTree, err
}
