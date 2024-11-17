package nat

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func (nc *NATSConnectionImpl) JetStream(options ...nats.JSOpt) (JetStreamContext, error) {
	js, err := nc.Conn.JetStream(options...)
	if err != nil {
		return nil, err
	}
	return &JetStreamContextImpl{JS: js}, nil
}

func (nc *NATSConnectionImpl) Close() {
	nc.Conn.Close()
}

func (nc *NATSConnectionImpl) GetConn() *nats.Conn {
	return nc.Conn
}

func (uc *URLConnector) Connect(enableLogging bool) (NATSConnection, JetStreamContext, error) {
	nc, err := nats.Connect(uc.URL, nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(time.Second),
		nats.ReconnectHandler(func(_ *nats.Conn) {}))
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, err
	}

	if enableLogging {
		log.Println("Connected to NATS with JetStream enabled at", uc.URL)
	}

	return &NATSConnectionImpl{Conn: nc}, &JetStreamContextImpl{JS: js}, nil
}
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
func (js *JetStreamContextImpl) Publish(subject string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return js.JS.Publish(subject, data, opts...)
}

func (js *JetStreamContextImpl) PublishAsync(subject string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return js.JS.PublishAsync(subject, data, opts...)
}

func (js *JetStreamContextImpl) SubscribeSync(subj string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return js.JS.SubscribeSync(subj, opts...)
}

func (js *JetStreamContextImpl) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return js.JS.Subscribe(subj, cb, opts...)
}

func (sm *SubscriptionManagerImpl) SetupConsumer(streamName, consumerName, durableName string) error {
	_, err := sm.JetStream.ConsumerInfo(streamName, consumerName)
	if err != nil {
		_, err = sm.JetStream.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:        durableName,
			AckPolicy:      nats.AckNonePolicy,
			AckWait:        30 * time.Second,
			DeliverSubject: consumerName,
		})
		if err != nil {
			return err
		}
		log.Printf("Consumer %s is ready", consumerName)
	} else {
		log.Printf("Consumer %s already exists", consumerName)
	}

	return nil
}

func (sm *SubscriptionManagerImpl) SubscribeToSubject(subject, durableName string) (*nats.Subscription, error) {
	sub, err := sm.JetStream.SubscribeSync(subject, nats.Durable(durableName))
	if err != nil {
		return nil, err
	}
	log.Printf("Subscribed to subject %s with durable %s", subject, durableName)
	return sub, nil
}

func (sm *SubscriptionManagerImpl) SubscribeAsyncWithHandler(subject, durableName string, handler MessageHandler, logger *log.Logger) error {
	_, err := sm.JetStream.Subscribe(subject, func(msg *nats.Msg) {
		logger.Printf("Received message: %s", string(msg.Data))

		err := handler.HandleMessage(msg.Data, logger)
		if err != nil {
			logger.Printf("Error handling message: %v", err)
		}

		msg.Ack()
	}, nats.Durable(durableName), nats.ManualAck())

	return err
}
