package whisper

import "context"

type Handler func(context.Context, interface{}) error

var TopicOptions = []Handler{}

// Send send msg to MQ
func Send(ctx context.Context, msg interface{}) error {
	for _, h := range TopicOptions {
		err := h(ctx, msg)
		if err != nil {
			return err
		}
	}
	return nil
}
