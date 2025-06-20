package agent

import (
	"errors"
	"regexp"
	"strings"

	"github.com/G-Sacker/kafka-lib/mq"
	"github.com/IBM/sarama"
)

var reIpPort = regexp.MustCompile(`^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}:[1-9][0-9]*$`)

type Config struct {
	Address        string `json:"address" required:"true"`
	Version        string `json:"version"` // e.g 2.1.0
	MQCert         string `json:"mq_cert"`
	OTEL           bool   `json:"otel"` // Whether otel tracing is enabled
	Username       string `json:"user_name"`
	Password       string `json:"password"`
	Algorithm      string `json:"algorithm"`
	SkipCertVerify bool   `json:"skip_cert_verify"`
}

func (cfg *Config) Validate() error {
	if r := cfg.parseAddress(); len(r) == 0 {
		return errors.New("invalid mq address")
	}

	if cfg.Algorithm != "sha512" && cfg.Algorithm != "sha256" && cfg.Algorithm != "" {
		return errors.New("invalid mq algorithm")
	}

	return nil
}

func (cfg *Config) mqConfig() mq.MQConfig {
	return mq.MQConfig{
		Addresses: cfg.parseAddress(),
	}
}

func (cfg *Config) parseAddress() []string {
	v := strings.Split(cfg.Address, ",")
	r := make([]string, 0, len(v))
	for i := range v {
		if reIpPort.MatchString(v[i]) {
			r = append(r, v[i])
		}
	}

	return r
}

func (cfg *Config) parseVersion() sarama.KafkaVersion {
	for _, sv := range sarama.SupportedVersions {
		if cfg.Version == sv.String() {
			return sv
		}
	}

	return sarama.MaxVersion
}
