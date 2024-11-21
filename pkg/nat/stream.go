package nat

import "github.com/nats-io/nats.go"

func (js *JetStreamContextImpl) StreamInfo(name string) (*nats.StreamInfo, error) {
	return js.JS.StreamInfo(name)
}

func (js *JetStreamContextImpl) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	return js.JS.AddStream(cfg)
}
