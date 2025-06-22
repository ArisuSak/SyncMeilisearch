package nat

import "github.com/nats-io/nats.go"


func (js *JetStreamContextImpl) Publish(subject string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return js.JS.Publish(subject, data, opts...)
}

func (js *JetStreamContextImpl) PublishAsync(subject string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return js.JS.PublishAsync(subject, data, opts...)
}
