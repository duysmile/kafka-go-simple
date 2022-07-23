package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/duysmile/kafka-go-simple/saramalib/pubsub"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"time"
)

type (
	SrmSubscriber struct {
		ctx          context.Context
		cancelFn     context.CancelFunc
		logger       *zap.Logger
		brokers      []string
		topics       []string
		groupID      string
		srmConfig    *sarama.Config
		ssg          *srmSubscriberGroup
		workerNumber int
	}

	subscriberGroupHandler struct {
		msgCh chan pubsub.SubscriberMessage
		ready chan bool
	}

	srmSubscriberGroup struct {
		ctx             context.Context
		cancelFn        context.CancelFunc
		logger          *zap.Logger
		topics          []string
		subscriberGroup sarama.ConsumerGroup
	}

	srmSubscriberMessage struct {
		*sarama.ConsumerMessage
		session sarama.ConsumerGroupSession
	}
)

func (s *subscriberGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(s.ready)
	return nil
}

func (s *subscriberGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (s *subscriberGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		s.msgCh <- &srmSubscriberMessage{
			ConsumerMessage: msg,
			session:         session,
		}
	}

	return nil
}

func loadDefaultSubscriber() *SrmSubscriber {
	srmConfig := sarama.NewConfig()
	srmConfig.Version = sarama.V2_2_1_0
	srmConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	srmConfig.Consumer.Return.Errors = true
	srmConfig.Consumer.Group.Session.Timeout = 30 * time.Second

	return &SrmSubscriber{
		brokers:      []string{"127.0.0.1:9092"},
		srmConfig:    srmConfig,
		workerNumber: 1,
	}
}

func NewSrmSubscriber(
	logger *zap.Logger,
	brokers []string,
	groupID string,
) *SrmSubscriber {
	sub := loadDefaultSubscriber()
	sub.brokers = brokers
	sub.logger = logger
	sub.groupID = groupID

	sub.logger.Info(
		"initialize subscriber",
		zap.Any("brokers", brokers),
		zap.Any("groupID", groupID),
	)

	return sub
}

func (sub *SrmSubscriber) RegisterTopic(topic string) {
	sub.topics = append(sub.topics, topic)
}

func (sub *SrmSubscriber) Run(ctx context.Context) (<-chan pubsub.SubscriberMessage, error) {
	sub.ctx, sub.cancelFn = context.WithCancel(ctx)
	subscriberGroup, err := sarama.NewConsumerGroup(sub.brokers, sub.groupID, sub.srmConfig)
	if err != nil {
		sub.logger.Error("failed to initialized consumer", zap.Error(err))
		return nil, err
	}

	sub.ssg = &srmSubscriberGroup{
		ctx:             sub.ctx,
		cancelFn:        sub.cancelFn,
		logger:          sub.logger,
		topics:          sub.topics,
		subscriberGroup: subscriberGroup,
	}
	return sub.ssg.Run(), nil
}

func (sub *SrmSubscriber) Close(ctx context.Context) error {
	return sub.ssg.subscriberGroup.Close()
}

func (s *srmSubscriberGroup) Run() <-chan pubsub.SubscriberMessage {
	msgCh := make(chan pubsub.SubscriberMessage)
	sgh := &subscriberGroupHandler{
		msgCh: msgCh,
		ready: make(chan bool),
	}

	go func() {
		defer close(msgCh)
		for {
			select {
			case <-s.ctx.Done():
				s.logger.Info("terminate: done signal context")
				return
			default:
				if err := s.subscriberGroup.Consume(s.ctx, s.topics, sgh); err != nil {
					s.logger.Error("failed to consume message", zap.Error(err))
				}

				if err := s.ctx.Err(); err != nil {
					return
				}

				sgh.ready = make(chan bool)
			}
		}
	}()
	<-sgh.ready

	return msgCh
}

func (s *srmSubscriberMessage) Topic() string {
	return s.ConsumerMessage.Topic
}

func (s *srmSubscriberMessage) Value() []byte {
	return s.ConsumerMessage.Value
}

func (s *srmSubscriberMessage) Key() []byte {
	return s.ConsumerMessage.Key
}

func (s *srmSubscriberMessage) Done(metadata string) {
	s.session.MarkMessage(s.ConsumerMessage, metadata)
}

func (s *srmSubscriberMessage) Timestamp() *time.Time {
	if s.ConsumerMessage.Timestamp.IsZero() {
		return nil
	}
	ts := s.ConsumerMessage.Timestamp
	return &ts
}
