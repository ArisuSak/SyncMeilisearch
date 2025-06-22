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
