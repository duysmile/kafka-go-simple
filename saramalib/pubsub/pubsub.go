package pubsub

import (
	"golang.org/x/net/context"
	"time"
)

type (
	Publisher interface {
		Publish(ctx context.Context, topic string, message, key []byte) error
	}

	Subscriber interface {
		RegisterTopic(topic string)
		Run(ctx context.Context) (<-chan SubscriberMessage, error)
		Close(ctx context.Context) error
	}

	SubscriberMessage interface {
		Value() []byte
		Topic() string
		Key() []byte
		Done(metadata string)
		Timestamp() *time.Time
	}
)
