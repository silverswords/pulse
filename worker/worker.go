package worker

import (
	cecontext "github.com/silverswords/whisper/context"
	"io"
	"runtime"
	"sync"
)

type Parser interface {
	Parse(handler) (handler, error)
}

type Worker struct {
	workerQueue chan handler
	Parser
}

type handler interface {
	Do(interface{}) error
}

// bug in error handler
func (w Worker) Do(iface interface{}) error {
	errch := make(chan error, 1)
	closedCh := make(chan struct{})
	go func() {
		select {
		case <-closedCh:
			return
		default:
			for v := <-w.workerQueue; ; {
				h, err := w.Parser.Parse(v)
				if err != nil {
					errch <- err
				}

				if err = h.Do(iface); err != nil {
					errch <- err
				}
				if err != nil {
					closedCh <- struct{}{}
				}
			}
		}
	}()

	err := <-errch
	return err
}
func MaxParallel() {
	// Start Polling.
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var msg binding.Message
				var respFn protocol.ResponseFn
				var err error

				if c.responder != nil {
					msg, respFn, err = c.responder.Respond(ctx)
				} else if c.receiver != nil {
					msg, err = c.receiver.Receive(ctx)
					respFn = noRespFn
				}

				if err == io.EOF { // Normal close
					return
				}

				if err != nil {
					cecontext.LoggerFrom(ctx).Warnf("Error while receiving a message: %s", err)
					continue
				}

				if err := c.invoker.Invoke(ctx, msg, respFn); err != nil {
					cecontext.LoggerFrom(ctx).Warnf("Error while handling a message: %s", err)
				}
			}
		}()
	}
	wg.Wait()
}
