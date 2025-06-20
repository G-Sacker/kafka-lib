package agent

import (
	"encoding/json"
	"time"

	"github.com/G-Sacker/kafka-lib/mq"
)

func newPublisher(redis Redis, log mq.Logger, queueName string, mqInstance mq.MQ) *publisherImpl {
	if redis != nil && log != nil && queueName != "" {
		p := &publisherImpl{
			q:          &queueImpl{redis: redis, queueName: queueName},
			logger:     log,
			mqInstance: mqInstance,
			stop:       make(chan struct{}),
			stopped:    make(chan struct{}),
		}

		p.start()
		return p
	}
	return nil
}

func (agent *MQAgent) Publish(topic string, header map[string]string, msg []byte, opts ...mq.PublishOption) error {
	v := &mq.Message{
		Header: header,
		Body:   msg,
	}

	if agent.publisher != nil {
		return agent.publisher.publish(agent.mqInstance, topic, v, opts...)
	}

	return agent.mqInstance.Publish(topic, v, opts...)
}

// queue
type queue interface {
	push(*message) error
	pop() (message, error)
	isEmpty(error) bool
}

// publisherImpl
type publisherImpl struct {
	q          queue
	logger     mq.Logger
	mqInstance mq.MQ
	stop       chan struct{}
	stopped    chan struct{}
}

func (impl *publisherImpl) publish(mqInstance mq.MQ, topic string, msg *mq.Message, opts ...mq.PublishOption) error {
	if err := mqInstance.Publish(topic, msg, opts...); err == nil {
		return nil
	}

	return impl.q.push(&message{
		Topic: topic,
		Msg:   *msg,
	})
}

func (impl *publisherImpl) start() {
	go impl.watch()
}

func (impl *publisherImpl) exit() {
	close(impl.stop)

	<-impl.stopped
}

func (impl *publisherImpl) watch() {
	var timer *time.Timer

	defer func() {
		if timer != nil {
			timer.Stop()
		}

		close(impl.stopped)
	}()

	tenMillisecond := 10 * time.Millisecond

	for {
		interval := time.Minute

		if msg, err := impl.q.pop(); err != nil {
			if !impl.q.isEmpty(err) {
				impl.logger.Error("failed to pop message, err: %s", err.Error())

				interval = tenMillisecond
			}
		} else {
			interval = tenMillisecond

			if err := impl.publish(impl.mqInstance, msg.Topic, &msg.Msg); err != nil {
				impl.logger.Error("faield to publish message, err:%s", err.Error())
			}
		}

		// time starts.
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-impl.stop:
			return

		case <-timer.C:
		}
	}
}

// message
type message struct {
	Topic string     `json:"topic"`
	Msg   mq.Message `json:"msg"`
}

func (do *message) MarshalBinary() ([]byte, error) {
	return json.Marshal(do)
}

func (do *message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, do)
}
