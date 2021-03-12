package subscription

import (
	"context"
	"crypto/tls"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/valyala/fasthttp"
	"sync/atomic"
)

// WithWebHook would turn on the ack function.
func WithWebHook(webhookHandler func(context.Context, *protocol.CloudEventsEnvelope), ssl bool) Option {
	return func(s *Subscription) error {
		// todo: consider remove it from the subscription and just use closure instead.
		s.webhookClient = &fasthttp.Client{
			MaxConnsPerHost:           512,
			ReadTimeout:               DefaultRecieveSettings.WebHookRequestTimeout,
			MaxIdemponentCallAttempts: 0,
		}
		if ssl {
			s.webhookClient.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}

		s.handlers = append(s.handlers, func(ctx context.Context, msg *protocol.CloudEventsEnvelope) {
			if msg.WebhookURL == "" {
				return
			}
			// user-defined handler.
			webhookHandler(ctx, msg)
			req := fasthttp.AcquireRequest()
			req.SetRequestURI(msg.WebhookURL)
			req.Header.SetContentType(msg.DataContentType)
			req.SetBody(msg.Data)

			resp := fasthttp.AcquireResponse()
			err := s.webhookClient.DoTimeout(req, resp, s.ReceiveSettings.WebHookRequestTimeout)
			defer func() {
				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)
			}()
			if err != nil {
				// dosomething.
				log.Error(err)
			}
		})
		return nil
	}
}

func WithMiddlewares(handlers ...func(context.Context, *protocol.CloudEventsEnvelope)) Option {
	return func(s *Subscription) error {
		s.handlers = append(s.handlers, handlers...)
		return nil
	}
}

func WithCount() Option {
	return func(s *Subscription) error {
		var count uint64
		s.handlers = append(s.handlers, func(ctx context.Context, m *protocol.CloudEventsEnvelope) {
			atomic.AddUint64(&count, 1)
			log.Info("count: ", count)
		})
		return nil
	}
}

func WithAutoACK() Option {
	return func(s *Subscription) error {
		s.EnableAck = true
		return nil
	}
}

type Option func(*Subscription) error

func (s *Subscription) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}
