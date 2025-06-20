package kafka

import (
	"context"
	"errors"
	"sync"

	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"github.com/opensourceways/server-common-lib/utils"

	"github.com/opensourceways/kafka-lib/mq"
)

type eventHandler interface {
	handle(*event)
}

// groupConsumer represents a Sarama consumer group consumer
type groupConsumer struct {
	mqOpts      *mq.Options
	subOpts     *mq.SubscribeOptions
	handler     eventHandler
	notifyReady func()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (gc *groupConsumer) Setup(sarama.ConsumerGroupSession) error {
	gc.notifyReady()

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (gc *groupConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a groupConsumer loop of ConsumerGroupClaim's Messages().
func (gc *groupConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log := gc.mqOpts.Log
	unmarshal := gc.mqOpts.Codec.Unmarshal

	for {
		select {
		case message := <-claim.Messages():
			msg := new(mq.Message)
			if err := unmarshal(message.Value, msg); err != nil {
				log.Errorf("unmarshal msg failed, err: %v", err)
			} else {
				gc.handler.handle(&event{
					m:    msg,
					km:   message,
					sess: session,
				})
			}

			if gc.subOpts.AutoAck {
				session.MarkMessage(message, "")
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// newSubscriber
func newSubscriber(
	topics []string,
	cli sarama.Client, cg sarama.ConsumerGroup,
	mqOpts *mq.Options, subOpts *mq.SubscribeOptions,
) *subscriber {
	return &subscriber{
		mqOpts:  mqOpts,
		subOpts: *subOpts,

		cli: cli,
		cg:  cg,

		topics: topics,

		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
}

// subscriber
type subscriber struct {
	mqOpts  *mq.Options
	subOpts mq.SubscribeOptions

	cli sarama.Client
	cg  sarama.ConsumerGroup

	topics []string

	once  sync.Once
	ready chan struct{}
	done  chan struct{}

	cancel context.CancelFunc
}

func (s *subscriber) Options() mq.SubscribeOptions {
	return s.subOpts
}

func (s *subscriber) Topics() []string {
	return s.topics
}

func (s *subscriber) Unsubscribe() error {
	mErr := utils.MultiError{}

	s.once.Do(func() {
		s.cancel()

		// wait
		<-s.done

		mErr.AddError(s.cg.Close())

		mErr.AddError(s.cli.Close())
	})

	return mErr.Err()
}

func (s *subscriber) start(handler eventHandler) error {
	f := func(ctx context.Context) {
		defer close(s.done)

		log := s.mqOpts.Log

		var gc sarama.ConsumerGroupHandler

		gc = &groupConsumer{
			mqOpts:      s.mqOpts,
			subOpts:     &s.subOpts,
			handler:     handler,
			notifyReady: s.notifyReady,
		}

		if s.mqOpts.Otel {
			log.Infof("otel comsumer wrapper enabled for %v", s.topics)
			gc = otelsarama.WrapConsumerGroupHandler(gc)
		}

		for {
			if err := s.cg.Consume(ctx, s.topics, gc); err != nil {
				log.Errorf("Consume topics:%v err: %s", s.topics, err.Error())

				return
			}

			if err := ctx.Err(); err != nil {
				log.Infof("exit by unsubscribing, err:%s", err.Error())

				return
			}

			log.Warn("maybe, server-side rebalance happened.")
		}
	}

	ctx, cancel := context.WithCancel(s.subOpts.Context)

	go f(ctx)

	select {
	case <-s.ready:
		s.cancel = cancel

		return nil

	case <-s.done:
		cancel()

		return errors.New("start failed")
	}
}

func (s *subscriber) notifyReady() {
	select {
	case <-s.ready:
		s.mqOpts.Log.Info("ready is closed")
	default:
		close(s.ready)
	}
}
