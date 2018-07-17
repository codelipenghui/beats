package pulsar

import (
	"github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

type message struct {
	msg pulsar.ProducerMessage

	topic   string
	key     string
	payload []byte
}

func (m *message) initProducerMessage() {
	m.msg = pulsar.ProducerMessage{
		Key:     m.key,
		Payload: m.payload,
	}
}
