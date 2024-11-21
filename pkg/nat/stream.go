package nat

import "github.com/nats-io/nats.go"

func (js *JetStreamContextImpl) StreamInfo(name string) (*nats.StreamInfo, error) {
	return js.JS.StreamInfo(name)
}

func (js *JetStreamContextImpl) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	return js.JS.AddStream(cfg)
}

func (js *JetStreamContextImpl) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	return js.JS.ConsumerInfo(stream, consumer)
}

func (js *JetStreamContextImpl) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	return js.JS.AddConsumer(stream, cfg)
}
