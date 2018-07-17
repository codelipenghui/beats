package pulsar

import (
	"time"
	"runtime"
	"errors"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

type pulsarConfig struct {
	URL                    string        `config:"url"  validate:"required"`
	IOThreads              int           `config:"io_threads"`
	Timeout                time.Duration `config:"timeout" validate:"min=1"`
	MessageListenerThreads int           `config:"message_listener_threads"`
	BulkMaxSize            int           `config:"bulk_max_size"`
	MaxRetries             int           `config:"max_retries"         validate:"min=-1,nonzero"`
	Codec                  codec.Config  `config:"codec"`
}

func defaultConfig() pulsarConfig {
	return pulsarConfig{
		URL:                    nil,
		IOThreads:              runtime.NumCPU() / 2,
		Timeout:                -1,
		MessageListenerThreads: runtime.NumCPU() / 2,
	}
}

func (c *pulsarConfig) Validate() error {
	if len(c.URL) == 0 {
		return errors.New("no pulsar url configured")
	}
	return nil
}

func newPulsarOptions(config *pulsarConfig) *pulsar.ClientOptions {
	co := &pulsar.ClientOptions{
		URL:                    config.URL,
		IOThreads:              config.IOThreads,
		MessageListenerThreads: config.MessageListenerThreads,
	}
	if config.Timeout > 0 {
		co.OperationTimeoutSeconds = config.Timeout
	}
	return co
}
