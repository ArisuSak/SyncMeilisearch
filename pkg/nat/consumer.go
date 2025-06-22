package nat

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func (js *JetStreamContextImpl) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	return js.JS.ConsumerInfo(stream, consumer)
}

func (js *JetStreamContextImpl) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	return js.JS.AddConsumer(stream, cfg)
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
