```
go get github.com/xaionaro-go/slowsync
go install github.com/xaionaro-go/slowsync/cmd/slowsync
`go env GOPATH`/bin/slowsync -src-broken-files /tmp/1-brokenfiles.txt -src-filetree-cache /tmp/1.cache /tmp/1 /tmp/2
```

```
$ `go env GOPATH`/bin/slowsync
slowsync [options] <dir-from> <dir-to>

$ `go env GOPATH`/bin/slowsync --help
Usage of /home/xaionaro/go/bin/slowsync:
  -dry-run
        do not copy anything
  -dst-filetree-cache string
        enables the file tree cache of the destination and set the path where to store it
  -src-broken-files string
        enables the list of broken files and set the path to it
  -src-filetree-cache string
        enables the file tree cache of the source and set the path where to store it
```
