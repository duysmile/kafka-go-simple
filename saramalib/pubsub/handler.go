package pubsub

import (
	"golang.org/x/net/context"
	"sync"
)

type KafkaHandler struct {
	sub             Subscriber
	mapTopicHandler map[string]Handler
	locker          sync.Mutex
}

type Handler interface {
	Consume(msg SubscriberMessage)
	IsSync() bool
}

func NewKafkaConsumeRunner(sub Subscriber) *KafkaHandler {
	return &KafkaHandler{
		sub:             sub,
		mapTopicHandler: make(map[string]Handler),
	}
}

func (h *KafkaHandler) Register(topic string, handler Handler) {
	h.locker.Lock()
	defer h.locker.Unlock()

	h.sub.RegisterTopic(topic)
	h.mapTopicHandler[topic] = handler
}

func (h *KafkaHandler) Run(ctx context.Context) error {
	cMsg, err := h.sub.Run(ctx)
	if err != nil {
		return err
	}

	go func(cMsg <-chan SubscriberMessage) {
		for msg := range cMsg {
			handler, ok := h.mapTopicHandler[msg.Topic()]
			if !ok {
				continue
			}

			if handler.IsSync() {
				handler.Consume(msg)
			} else {
				go handler.Consume(msg)
			}
		}
	}(cMsg)

	return nil
}
