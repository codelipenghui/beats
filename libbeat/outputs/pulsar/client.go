package pulsar

import (
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/common/fmtstr"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"errors"
)

var (
	errNoTopicsSelected = errors.New("no topic could be selected")
)

type client struct {
	observer      outputs.Observer
	topic         outil.Selector
	key           *fmtstr.EventFormatString
	index         string
	codec         codec.Codec
	pulsarClient  pulsarClient
	clientOptions pulsar.ClientOptions
}

func NewPulsarOutputClient(
	observer outputs.Observer,
	index string,
	codec codec.Codec,
	clientOptions *pulsar.ClientOptions,
) (*client, error) {
	c := &client{
		observer:      observer,
		index:         index,
		codec:         codec,
		clientOptions: *clientOptions,
	}
	return c, nil
}

func (c *client) Connect() error {
	pc := NewPulsarClient(&c.clientOptions)
	err := pc.Connect()
	if err != nil {
		logp.Err("pulsar connect fails with: %v", err)
		return err
	}
	c.pulsarClient = *pc
	return nil
}

func (c *client) Close() error {
	return c.pulsarClient.Close()
}

func (c *client) Publish(batch publisher.Batch) error {
	defer batch.ACK()
	events := batch.Events()
	c.observer.NewBatch(len(events))
	for i := range events {
		event := &events[i]
		msg, err := c.getEventMessage(event)
		if err != nil {
			return err
		}
		c.pulsarClient.Send(msg.topic, msg.payload)
	}
	c.observer.Dropped(len(events))
	c.observer.Acked(len(events))
	return nil
}

func (c *client) getEventMessage(data *publisher.Event) (*message, error) {
	event := &data.Content
	msg := &message{}
	if event.Meta != nil {
		if value, ok := event.Meta["topic"]; ok {
			if topic, ok := value.(string); ok {
				msg.topic = topic
			}
		}
	}
	if msg.topic == "" {
		return nil, errNoTopicsSelected
	}
	serializedEvent, err := c.codec.Encode(c.index, event)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, len(serializedEvent))
	copy(buf, serializedEvent)
	msg.payload = buf
	return msg, nil
}
