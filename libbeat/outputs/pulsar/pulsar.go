package pulsar

import (
	gometrics "github.com/rcrowley/go-metrics"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/outputs/codec"
)

var pulsarMetricsRegistryInstance gometrics.Registry

type pulsarOutput struct {
	config pulsarConfig
}

func init() {
	reg := gometrics.NewPrefixedRegistry("libbeat.pulsar.")
	pulsarMetricsRegistryInstance = reg
	outputs.RegisterType("pulsar", makePulsar)
}

func pulsarMetricsRegistry() gometrics.Registry {
	return pulsarMetricsRegistryInstance
}

func makePulsar(
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}
	libCfg := newPulsarOptions(&config)
	codec, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}
	client, err := NewPulsarOutputClient(observer, beat.Beat, codec, libCfg)
	if err != nil {
		return outputs.Fail(err)
	}
	retry := 0
	if config.MaxRetries < 0 {
		retry = -1
	}
	return outputs.Success(config.BulkMaxSize, retry, client)
}
