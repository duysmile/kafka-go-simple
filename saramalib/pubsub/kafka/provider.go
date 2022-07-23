package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"time"
)

type SrmPublisher struct {
	brokers   []string
	logger    *zap.Logger
	srmConfig *sarama.Config
	producer  sarama.SyncProducer
}

func loadDefaultProducer() *SrmPublisher {
	srmConfig := sarama.NewConfig()
	srmConfig.Version = sarama.V2_2_1_0
	srmConfig.Producer.Timeout = 30 * time.Second
	srmConfig.Producer.Return.Errors = true
	srmConfig.Producer.Return.Successes = true
	srmConfig.Producer.Retry.Max = 5
	srmConfig.Producer.RequiredAcks = sarama.WaitForAll

	return &SrmPublisher{
		brokers:   []string{"127.0.0.1:9092"},
		srmConfig: srmConfig,
	}
}

func NewSrmProducer(logger *zap.Logger, brokers []string) (*SrmPublisher, error) {
	pub := loadDefaultProducer()
	pub.brokers = brokers
	pub.logger = logger
	pub.logger.Info(
		"initialize producer",
		zap.Any("brokers", brokers),
	)

	producer, err := sarama.NewSyncProducer(brokers, pub.srmConfig)
	if err != nil {
		pub.logger.Error("failed to initialize producer", zap.Error(err))
		return nil, err
	}

	pub.producer = producer
	return pub, nil
}

func (pub *SrmPublisher) Publish(ctx context.Context, topic string, message []byte, key []byte) error {
	partition, offset, err := pub.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(message),
		Timestamp: time.Now().UTC(),
	})

	if err != nil {
		return err
	}
	pub.logger.Info(fmt.Sprintf(
		"publish message in topic [%v] successfully with key [%v], value [%v], partition [%v], offset [%v]",
		topic,
		string(key),
		string(message),
		partition,
		offset,
	))
	return nil
}
