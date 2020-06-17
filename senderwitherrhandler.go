package whisper

import "log"

type RetryErr struct {
	msg *Message
	retrytime int
}

type Errorsender struct {
	errQueue *Queue
	closed bool
	s *Sender

	retrytime int
}

func NewErrorsender(driver Driver,retrytime int) *Errorsender {
	return &Errorsender{
		errQueue: NewQueue(),
		s:NewSender(driver),
		retrytime: retrytime,
	}
}

func (s *Errorsender) Send(msg *Message) {
	s.s.Send(msg)
}

// two goroutines to send message and handle error
func (s *Errorsender) execution()  {
	go func() {
		for !s.closed {
			err,ok := s.errQueue.Pop().(Handler)
			if !ok {
				log.Println("[err]: an error is not Handler, can't handle. \n [content]: ", err)
				continue
			}
			
			err.Do(s.s.driver)
		}
	}()

	go func(){
		for {
			select {
			case msg := <-s.s.q:
				{
					log.Println("sender send: ",msg)
					//_ = msg.Do(s.driver)

					err := msg.Do(s.s.driver)
					if err != nil {
						if err == RetryError {
							s.errQueue.Append()
						}
						s.errQueue.Append(err)
					}
				}
			case _ = <-s.s.closed:
				return
			}
		}
	}()

}

func (s *Errorsender) Close() {
	s.closed = true
	s.s.Close()
}