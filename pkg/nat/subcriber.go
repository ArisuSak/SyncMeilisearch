package nat

import (
	"log"

	"github.com/nats-io/nats.go"
)

func (js *JetStreamContextImpl) SubscribeSync(subj string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return js.JS.SubscribeSync(subj, opts...)
}

func (js *JetStreamContextImpl) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return js.JS.Subscribe(subj, cb, opts...)
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
