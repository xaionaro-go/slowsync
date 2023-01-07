package osrecovery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
)

const (
	newNameTimeout = time.Hour
)

type listOutputParser struct {
	NameCh              chan string
	ErrCh               chan error
	CurrentFieldNum     int
	FieldValue          []byte
	lastNewNameTSLocker sync.Mutex
	lastNewNameTS       time.Time
	nameCount           map[string]int
	logger              logger.Logger
	cancelFn            context.CancelFunc
	status              int
	wgWriteLock         sync.Mutex
	wgWatchDog          sync.WaitGroup
}

var _ io.Writer = (*listOutputParser)(nil)

func newListOutputParser(ctx context.Context) (context.Context, *listOutputParser) {
	p := &listOutputParser{
		nameCount:     map[string]int{},
		lastNewNameTS: time.Now(),
	}
	ctx = p.start(ctx)
	p.logger = logger.FromCtx(ctx)
	return ctx, p
}

func (p *listOutputParser) start(ctx context.Context) context.Context {
	p.NameCh = make(chan string)
	p.ErrCh = make(chan error)
	ctx, p.cancelFn = context.WithCancel(ctx)
	return ctx
}

func (p *listOutputParser) StartWatchDog(ctx context.Context) {
	p.wgWatchDog.Wait()
	p.wgWatchDog.Add(1)
	go func() {
		defer p.wgWatchDog.Done()
		p.watchDog(ctx)
	}()
}

func (p *listOutputParser) watchDog(ctx context.Context) {
	ticker := time.NewTicker(newNameTimeout / 50)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ts := p.LastNewNameTS()
		if time.Since(ts) > newNameTimeout {
			p.ErrCh <- fmt.Errorf("watchdog timeout")
			p.cancelFn()
			return
		}
	}
}

func (p *listOutputParser) Close() {
	p.wgWatchDog.Wait()
	p.wgWriteLock.Lock()
	defer p.wgWriteLock.Unlock()
	close(p.NameCh)
	close(p.ErrCh)
}

func (p *listOutputParser) LastNewNameTS() time.Time {
	p.lastNewNameTSLocker.Lock()
	ts := p.lastNewNameTS
	p.lastNewNameTSLocker.Unlock()
	return ts
}

var fieldSeparator = []byte{0}

func (p *listOutputParser) Write(b []byte) (int, error) {
	p.wgWriteLock.Lock()
	defer p.wgWriteLock.Unlock()

	n := 0
	parseFieldSeparatorType := func() {
		p.status = 0
		switch p.CurrentFieldNum {
		case 4:
			name := string(p.FieldValue)
			count := p.nameCount[name]
			count++
			p.nameCount[name] = count
			switch {
			case count == 1:
				func() {
					p.lastNewNameTSLocker.Lock()
					defer func() {
						p.lastNewNameTS = time.Now()
						p.lastNewNameTSLocker.Unlock()
					}()
					p.NameCh <- name
				}()
			case count > 10:
				p.ErrCh <- fmt.Errorf("name '%s' is duplicated (count: %d), cancelling the dir-scanning", name, count)
				p.cancelFn()
			default:
				p.ErrCh <- fmt.Errorf("name '%s' is duplicated (count: %d)", name, count)
			}
		}
		switch b[0] {
		case '\t':
			p.CurrentFieldNum++
		case '\n':
			p.CurrentFieldNum = 0
		default:
			err := fmt.Errorf("invalid separator type: %d (0x%02X) '%c'", b[0], b[0], b[0])
			p.logger.Errorf("%s", err)
			panic(err)
		}
		n++
		b = b[1:]
		p.FieldValue = p.FieldValue[:0]
	}

	for len(b) > 0 {
		if p.status == 1 {
			parseFieldSeparatorType()
		}
		if len(b) == 0 {
			break
		}

		idx := bytes.Index(b, fieldSeparator)
		if idx == -1 {
			p.FieldValue = append(p.FieldValue, b...)
			n += len(b)
			return n, nil
		}
		n += idx + 1
		p.FieldValue = append(p.FieldValue, b[:idx]...)
		b = b[idx+1:]
		p.status = 1
		if len(b) == 0 {
			return n, nil
		}
	}

	return n, nil
}
