package pubsub

import (
	"golang.org/x/net/context"
	"sync"
)

type KafkaHandler struct {
	ctx             context.Context
	sub             Subscriber
	mapTopicHandler map[string]Handler
	locker          sync.Mutex
}

type Handler interface {
	Consume(msg SubscriberMessage)
	IsSync() bool
}

func NewKafkaConsumeRunner(ctx context.Context, sub Subscriber) *KafkaHandler {
	return &KafkaHandler{
		ctx:             ctx,
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

func (h *KafkaHandler) Run() error {
	cMsg, err := h.sub.Run(h.ctx)
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
