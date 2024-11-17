package nat

import (
	"log"

	"github.com/nats-io/nats.go"
)

type NATSConnection interface {
	JetStream(options ...nats.JSOpt) (JetStreamContext, error)
	Close()
	GetConn() *nats.Conn
}

type JetStreamContext interface {
	StreamInfo(name string) (*nats.StreamInfo, error)
	AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error)
	ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error)
	AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error)
	Publish(subject string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	SubscribeSync(subj string, opts ...nats.SubOpt) (*nats.Subscription, error)
	Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error)
	PublishAsync(subject string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error)
}

type SubscriptionManagerImpl struct {
	JetStream JetStreamContext
}

type NATSConnector interface {
	Connect(enableLogging bool) (NATSConnection, JetStreamContext, error)
}
type NATSConnectionImpl struct {
	Conn *nats.Conn
}

type JetStreamContextImpl struct {
	JS nats.JetStreamContext
}

type URLConnector struct {
	URL string
}

type MessageHandler interface {
	HandleMessage([]byte, *log.Logger) error
}
