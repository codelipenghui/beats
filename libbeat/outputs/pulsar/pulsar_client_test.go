package pulsar

import (
	"testing"
	"github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
	"fmt"
)

func TestSend(t *testing.T) {
	c := NewPulsarClient(&pulsar.ClientOptions{URL: "pulsar://39.105.111.52:6650"})
	c.Connect()
	err := c.Send("pulsar-test", []byte("hello"))
	fmt.Println(err)
}
