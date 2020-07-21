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

func (w Worker) Do(iface interface{}) error {
	errch := make(chan error)
	go func() {
		select {
		case <-errch:
			return
		default:
			for v := <-w.workerQueue; ; {
				h, err := w.Parser.Parse(v)
				if err != nil {
					errch <- err
				}

				if err = h.Do(); err != nil {
					errch <- err
				}
			}
		}
	}()

	err := <-errch
	return err
}
