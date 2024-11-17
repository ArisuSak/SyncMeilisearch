package nat

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func SetupNATS(ctx context.Context, streamName string, subject string, logger *log.Logger) (*nats.Conn, nats.JetStreamContext, error) {
    nc, js, err := ConnectToKubernetesNATS(true)
    if err != nil {
        logger.Fatalf("Error starting NATS server: %v", err)
    }
    logger.Println("Connected to NATS server")

    err = SetupJetStream(js, streamName, subject)
    if err != nil {
        logger.Fatalf("Error setting up JetStream: %v", err)
    }

    return nc, js, nil
}

// local embedded server
func RunEmbeddedServer(inProcess bool, enableLogging bool) (*nats.Conn, *server.Server, error) {
	opts := &server.Options{
		DontListen: inProcess,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
	return nil, nil, err
	}

	if enableLogging {
		ns.ConfigureLogger()
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, errors.New("nats server did not start")
	}

	clientOpts := []nats.Option{}
	if inProcess {
		clientOpts = append(clientOpts, nats.InProcessServer(ns))
	}

	nc, err := nats.Connect(ns.ClientURL(), clientOpts...)
	if err != nil {
		return nil, nil, err
	}
	
	return nc, ns, nil

}

// connect to kubernetes nats
func ConnectToKubernetesNATS(enableLogging bool) (*nats.Conn, nats.JetStreamContext, error) {
    kubernetesNATSURL := "http://localhost:4222"

    nc, err := nats.Connect(kubernetesNATSURL,   nats.RetryOnFailedConnect(true),
    nats.MaxReconnects(10),
    nats.ReconnectWait(time.Second),
    nats.ReconnectHandler(func(_ *nats.Conn) {}),
        // Note that this will be invoked for the first asynchronous connect.
    )
    if err != nil {
        return nil, nil, err
	}

	js, err := nc.JetStream()
    if err != nil {
        nc.Close()
        return nil, nil, err
    }

    if enableLogging {
        log.Println("Connected to NATS with JetStream enabled")
    }
	
    return nc, js, nil
}

// SetupJetStream sets up a JetStream stream and consumer
func SetupJetStream(js nats.JetStreamContext, streamName string, subject string) error {
    stream, err := js.StreamInfo(streamName)
    if err != nil {
        _, err = js.AddStream(&nats.StreamConfig{
            Name:     streamName,
            Subjects: []string{subject},
			// Retention: nats.WorkQueuePolicy,
        })
        if err != nil && err != nats.ErrStreamNameAlreadyInUse {
            return err
        }
        log.Printf("Stream %s is ready", streamName)
    } else {
        log.Printf("Stream %s already exists", stream.Config.Name)
    }

	return nil
}

// SetupConsumer sets up a JetStream consumer
func SetupConsumer(js nats.JetStreamContext, streamName string, consumerName string, durableName string) error {
	_, err := js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:   durableName,
			AckPolicy: nats.AckNonePolicy,
			// AckPolicy:      nats.AckExplicitPolicy,
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

// PublishMessage publishes a message using JetStream
func PublishMessage(js nats.JetStreamContext, subject string, message []byte) error {
	_, err := js.Publish(subject, message)
	if err != nil {
		return err
	}

	log.Printf("Published message to subject '%s': %s", subject, message)
	return nil
}

// PublishAsyn
func PublishAsync(js nats.JetStreamContext, subject string, message []byte) error {
    _, err := js.PublishAsync(subject, message)
    if err != nil {
        return err
    }

	log.Printf("Published asyn message to subject '%s': %s", subject, message)
    return nil
}

// SubscribeToSubject subscribes to a subject using JetStream
func SubscribeToSubject(js nats.JetStreamContext, subject string, durableName string) (*nats.Subscription, error) {
	sub, err := js.SubscribeSync(subject, nats.Durable(durableName))
	if err != nil {
		return nil, err
	}
	log.Printf("Subscribed to subject %s with durable %s", subject, durableName)
	return sub, nil
}

// SubcribeAsyn
func SubcribeAsyn(js nats.JetStreamContext, subject, durableName string, logger *log.Logger) (chan []byte, error) {
	msgCh := make(chan []byte)
    _, err := js.Subscribe(subject, func(msg *nats.Msg) {
        logger.Printf("Received message: %s", string(msg.Data))

		msgCh <- msg.Data
	
        msg.Ack() 

    }, nats.Durable(durableName), nats.ManualAck())

    if err != nil {
        return nil, err
    }
    return msgCh, nil
}

