package mq

import (
	"context"
	"crypto/tls"

	"github.com/IBM/sarama"
)

const (
	StrategyKindRetry    = "retry"
	StrategyKindDoOnce   = "do_once"
	StrategyKindSendBack = "send_back"
)

var (
	StrategyRetry    = strategyImpl(StrategyKindRetry)
	StrategyDoOnce   = strategyImpl(StrategyKindDoOnce)
	StrategySendBack = strategyImpl(StrategyKindSendBack)
)

type Strategy interface {
	Strategy() string
}

type strategyImpl string

func (impl strategyImpl) Strategy() string {
	return string(impl)
}

type Logger interface {
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}

type Options struct {
	Addresses []string
	Version   sarama.KafkaVersion
	Secure    bool
	Codec     Codecer
	Username  string
	Password  string
	Algorithm string

	// Handler executed when error happens in mq message processing
	ErrorHandler Handler

	TLSConfig *tls.Config

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context

	Log Logger
	// Whether otel tracing is enabled
	Otel bool
}

type Option func(*Options)

// Addresses set the host addresses to be used by the mq
func Addresses(addrs ...string) Option {
	return func(o *Options) {
		o.Addresses = addrs
	}
}

// user/password/algorithm are needed by SASL auth
func Sasl(user, pass, algorithm string) Option {
	return func(o *Options) {
		o.Username = user
		o.Password = pass
		o.Algorithm = algorithm
	}
}

// Version set the kafka version for sarama
func Version(version sarama.KafkaVersion) Option {
	return func(o *Options) {
		o.Version = version
	}
}

// Secure communication with the mq
func Secure(b bool) Option {
	return func(o *Options) {
		o.Secure = b
	}
}

// Codec sets the codec used for encoding/decoding used where
func Codec(c Codecer) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// ErrorHandler set the error handler
func ErrorHandler(h Handler) Option {
	return func(o *Options) {
		o.ErrorHandler = h
	}
}

// SetTLSConfig Specify TLS Config
func SetTLSConfig(t *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = t
	}
}

func Context(c context.Context) Option {
	return func(o *Options) {
		o.Context = c
	}
}

func ContextWithValue(k, v interface{}) Option {
	return func(o *Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}

		o.Context = context.WithValue(o.Context, k, v)
	}
}

func Log(log Logger) Option {
	return func(o *Options) {
		if log != nil {
			o.Log = log
		}
	}
}

func Otel(b bool) Option {
	return func(o *Options) {
		o.Otel = b
	}
}

type PublishOptions struct {
	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type PublishOption func(*PublishOptions)

// PublishContext set context
func PublishContext(ctx context.Context) PublishOption {
	return func(o *PublishOptions) {
		o.Context = ctx
	}
}

type SubscribeOptions struct {
	// AutoAck defaults to true. When a handler returns
	// with a nil error the message is receipt already.
	AutoAck bool

	// Subscribers with the same queue name
	// will create a shared subscription where each
	// receives a subset of messages.
	Queue string

	// RetryNum specifies the one that retry when handle failed
	RetryNum int

	// Strategy specifies the one for handling message
	Strategy Strategy

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type SubscribeOption func(*SubscribeOptions)

func NewSubscribeOptions(opts ...SubscribeOption) SubscribeOptions {
	opt := SubscribeOptions{
		AutoAck: true,
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

// DisableAutoAck will disable auto acking of messages
// after they have been handled.
func DisableAutoAck() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = false
	}
}

// Queue sets the name of the queue to share messages on
func Queue(name string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Queue = name
	}
}

// SubscribeContext set context
func SubscribeContext(ctx context.Context) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Context = ctx
	}
}

// SubscribeRetryNum sets RetryNum
func SubscribeRetryNum(v int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.RetryNum = v
	}
}

// SubscribeStrategy sets Strategy
func SubscribeStrategy(v Strategy) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Strategy = v
	}
}
