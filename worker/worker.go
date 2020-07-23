package worker

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
