package kafka

import (
	"fmt"
	"time"

	"github.com/G-Sacker/kafka-lib/mq"
)

func (impl *kfkMQ) genHanler(h mq.Handler, opt *mq.SubscribeOptions) eventHandler {
	log := impl.opts.Log

	eh := func(e mq.Event) {
		if eh := impl.opts.ErrorHandler; eh != nil {
			if err1 := eh(e); err1 != nil {
				log.Error(err1)
			}
		} else {
			log.Error(e.Error())
		}
	}

	v := ""
	if opt.Strategy != nil {
		v = opt.Strategy.Strategy()
	}

	switch v {
	case mq.StrategyKindDoOnce:
		return &doOnceHandler{
			handler: h,
			eh:      eh,
		}

	case mq.StrategyKindRetry:
		return &retryHandler{
			handler:  h,
			eh:       eh,
			retryNum: opt.RetryNum,
		}

	case mq.StrategyKindSendBack:
		return &sendBackHandler{
			handler: h,
			eh:      eh,
			log:     log,
			publish: impl.Publish,
		}

	default:
		return &doOnceHandler{
			handler: h,
			eh:      eh,
		}
	}
}

// errorHandler
type errorHandler func(mq.Event)

// doOnceHandler
type doOnceHandler struct {
	handler mq.Handler
	eh      errorHandler
}

func (h *doOnceHandler) handle(e *event) {
	if err := h.handler(e); err != nil {
		e.err = fmt.Errorf("handle event, err: %v", err)

		h.eh(e)
	}
}

// sendBackHandler
type sendBackHandler struct {
	handler mq.Handler
	eh      errorHandler
	log     mq.Logger
	publish func(topic string, msg *mq.Message, opts ...mq.PublishOption) error
}

func (h *sendBackHandler) handle(e *event) {
	if err := h.handler(e); err != nil {
		e.err = fmt.Errorf("handle event, err: %v", err)

		h.eh(e)

		// publish again
		if err := h.publish(e.Topic(), e.m); err != nil {
			h.log.Errorf("publish again failed, err:%s", err.Error())
		}
	}
}

// retryHandler
type retryHandler struct {
	handler  mq.Handler
	eh       errorHandler
	retryNum int
}

func (h *retryHandler) handle(e *event) {
	err := h.handler(e)
	if err == nil {
		return
	}

	// retry
	tenMillisecond := 10 * time.Millisecond
	timer := time.NewTimer(tenMillisecond)

	defer func() {
		timer.Stop()

		if err != nil {
			e.err = fmt.Errorf("handle event, err: %v", err)

			h.eh(e)
		}
	}()

	for i := 1; i < h.retryNum; i++ {
		timer.Reset(tenMillisecond)

		select {
		case <-e.sess.Context().Done():
			return

		case <-timer.C:
			if err = h.handler(e); err == nil {
				return
			}
		}
	}
}
