package pulsar

import (
	"github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
	"github.com/elastic/beats/libbeat/logp"
	"sync"
)

type pulsarClient struct {
	clientOptions pulsar.ClientOptions
	client        pulsar.Client
	producers     map[string]pulsar.Producer
	mutex         sync.Mutex
	wg            sync.WaitGroup
}

func NewPulsarClient(options *pulsar.ClientOptions) *pulsarClient {
	c := &pulsarClient{
		clientOptions: *options,
		producers:     make(map[string]pulsar.Producer),
	}
	return c
}

func (c *pulsarClient) Connect() error {
	client, err := pulsar.NewClient(c.clientOptions)
	if err != nil {
		logp.Err("pulsar connect fails with: %v", err)
		return err
	}
	c.client = client
	return nil
}

func (c *pulsarClient) Close() error {
	return c.client.Close()
}

type SendRes struct {
	msg pulsar.ProducerMessage
	err error
}

func (c *pulsarClient) Send(topic string, payload []byte) error {
	ch := make(chan *SendRes)
	c.SendAsync(topic, payload, func(res SendRes) {
		ch <- &res
	})
	sr := <-ch
	return sr.err
}

func (c *pulsarClient) SendAsync(topic string, payload []byte, callback func(res SendRes)) {
	c.mutex.Lock()
	producer := c.producers[topic]
	if producer == nil {
		option := pulsar.ProducerOptions{
			Topic:            topic,
			BlockIfQueueFull: true,
			Batching:         true,
		}
		p, err := c.client.CreateProducer(option)
		if err != nil {
			callback(SendRes{pulsar.ProducerMessage{}, err})
		}
		c.producers[topic] = p
		producer = p
	}
	c.mutex.Unlock()
	msg := pulsar.ProducerMessage{
		Payload: payload,
	}
	producer.SendAsync(nil, msg, func(message pulsar.ProducerMessage, e error) {
		callback(SendRes{message, e})
	})
}
