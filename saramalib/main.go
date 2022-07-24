package main

import (
	"context"
	"github.com/duysmile/kafka-go-simple/saramalib/pubsub"
	"github.com/duysmile/kafka-go-simple/saramalib/pubsub/kafka"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("failed to init logger", err)
	}

	brokers := []string{"localhost:9092"}

	pub, err := kafka.NewSrmProducer(logger, brokers)
	if err != nil {
		log.Fatal("failed to init producer", err)
	}

	sub := kafka.NewSrmSubscriber(logger, brokers, "kafka-test")

	mainCtx := context.Background()
	runner := pubsub.NewKafkaConsumeRunner(sub)

	testWorker := NewTestHandler(true)
	runner.Register("test-1", testWorker)
	runner.Register("test-2", testWorker)
	runner.Register("test-3", testWorker)

	if err := runner.Run(mainCtx); err != nil {
		log.Fatal("failed to consuming", err)
	}

	_ = pub.Publish(mainCtx, "test-1", []byte("hello"), []byte("world"))
	_ = pub.Publish(mainCtx, "test-2", []byte("hello"), []byte("world"))
	_ = pub.Publish(mainCtx, "test-3", []byte("hello"), []byte("world"))

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSEGV)
	<-c
}

type testHandler struct {
	isSync bool
}

func (t testHandler) Consume(msg pubsub.SubscriberMessage) {
	log.Println(
		msg.Topic(),
		string(msg.Key()),
		string(msg.Value()),
	)
	msg.Done("")
}

func (t testHandler) IsSync() bool {
	return t.isSync
}

func NewTestHandler(isSync bool) pubsub.Handler {
	return &testHandler{
		isSync,
	}
}
