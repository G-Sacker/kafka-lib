package agent

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/G-Sacker/kafka-lib/kafka"
	"github.com/G-Sacker/kafka-lib/mq"
)

var (
	mqInstance mq.MQ
	subscriber *serviceImpl
	publisher  *publisherImpl
)

type MQAgent struct {
	mqInstance mq.MQ
	subscriber *serviceImpl
	publisher  *publisherImpl
}

func newMQAgent(cfg *Config, log mq.Logger, redis Redis, queueName string, removeCert bool) (*MQAgent, error) {
	if log == nil {
		return nil, errors.New("missing log")
	}

	agent := &MQAgent{}

	v := mq.MQ(nil)

	if cfg.MQCert != "" {
		ca, err := ioutil.ReadFile(cfg.MQCert)
		if err != nil {
			return nil, err
		}

		if removeCert {
			if err := os.Remove(cfg.MQCert); err != nil {
				return nil, err
			}
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("failed to append certs from PEM")
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.SkipCertVerify,
			RootCAs:            pool,
		}

		v = kafka.NewMQ(
			mq.Addresses(cfg.mqConfig().Addresses...),
			mq.Version(cfg.parseVersion()),
			mq.Log(log),
			mq.Secure(true),
			mq.SetTLSConfig(tlsConfig),
			mq.Sasl(cfg.Username, cfg.Password, cfg.Algorithm),
			mq.Otel(cfg.OTEL),
		)
	} else {
		v = kafka.NewMQ(
			mq.Addresses(cfg.mqConfig().Addresses...),
			mq.Version(cfg.parseVersion()),
			mq.Log(log),
			mq.Otel(cfg.OTEL),
		)
	}

	if err := v.Init(); err != nil {
		return nil, err
	}

	if err := v.Connect(); err != nil {
		return nil, err
	}

	// mqInstance = v
	// subscriber = &serviceImpl{logger: log}

	// newPublisher(redis, log, queueName)

	agent.mqInstance = v
	agent.subscriber = &serviceImpl{logger: log}
	agent.publisher = newPublisher(redis, log, queueName, agent.mqInstance)

	return agent, nil
}

func (agent *MQAgent) Exit() {
	if subscriber != nil {
		subscriber.unsubscribe()

		subscriber = nil
	}

	if publisher != nil {
		publisher.exit()

		publisher = nil
	}

	if mqInstance != nil {
		if err := mqInstance.Disconnect(); err != nil {
			mqInstance.Options().Log.Errorf("exit kafka, err:%v", err)
		}

		mqInstance = nil
	}
}

func (agent *MQAgent) Subscribe(group string, h Handler, topics []string) error {
	if group == "" || h == nil || len(topics) == 0 {
		return errors.New("missing parameters")
	}

	if subscriber == nil {
		return errors.New("unimplemented")
	}

	return subscriber.subscribe(
		agent.mqInstance, h, topics,
		mq.Queue(group),
		mq.SubscribeStrategy(mq.StrategyDoOnce),
	)
}

func (agent *MQAgent) SubscribeWithStrategyOfRetry(group string, h Handler, topics []string, retryNum int) error {
	if group == "" || h == nil || len(topics) == 0 || retryNum == 0 {
		return errors.New("missing parameters")
	}

	if subscriber == nil {
		return errors.New("unimplemented")
	}

	return subscriber.subscribe(
		agent.mqInstance, h, topics,
		mq.Queue(group),
		mq.SubscribeRetryNum(retryNum),
		mq.SubscribeStrategy(mq.StrategyRetry),
	)
}

func (agent *MQAgent) SubscribeWithStrategyOfSendBack(group string, h Handler, topics []string) error {
	if group == "" || h == nil || len(topics) == 0 {
		return errors.New("missing parameters")
	}

	if subscriber == nil {
		return errors.New("unimplemented")
	}

	return subscriber.subscribe(

		agent.mqInstance, h, topics,
		mq.Queue(group),
		mq.SubscribeStrategy(mq.StrategySendBack),
	)
}

// Handler
type Handler func([]byte, map[string]string) error

// serviceImpl
type serviceImpl struct {
	subscribers []mq.Subscriber
	logger      mq.Logger
}

func (impl *serviceImpl) unsubscribe() {
	s := impl.subscribers
	for i := range s {
		if err := s[i].Unsubscribe(); err != nil {
			impl.logger.Errorf(
				"failed to unsubscribe to topic:%v, err:%v",
				s[i].Topics(), err,
			)
		}
	}
}

func (impl *serviceImpl) subscribe(mqInstance mq.MQ, h Handler, topics []string, opts ...mq.SubscribeOption) error {
	s, err := mqInstance.Subscribe(
		func(e mq.Event) error {
			msg := e.Message()
			if msg == nil {
				return nil
			}

			return h(msg.Body, msg.Header)
		},
		topics,
		opts...,
	)
	if err == nil {
		impl.subscribers = append(impl.subscribers, s)
	}

	return err
}
