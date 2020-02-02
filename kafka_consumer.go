package franz

import (
  "time"
  "context"
  "strings"
  "errors"
  "github.com/rs/zerolog/log"
  "github.com/segmentio/kafka-go"
  "github.com/crackgoland/env"
  _ "github.com/segmentio/kafka-go/snappy"
)

var (
  ErrKafkaConsumeInsufficientConfig = errors.New("Not enough data specified for KafkaConsumer in environment")
)

type KafkaConsumer struct {
  env     *env.Env
  config  kafka.ReaderConfig
  reader  *kafka.Reader
  Channel chan *kafka.Message
}

func NewKafkaConsumer(env *env.Env) (*KafkaConsumer, error) {

  kafkaBrokers, _ := env.Get("KAFKA_BROKERS", "kafka1:9092")
  kafkaTopic, _ := env.Get("KAFKA_TOPIC", "changeme")

  kafkaBrokersList := strings.Split(kafkaBrokers, ",")

  if len(kafkaBrokersList) == 0 {
    return nil, ErrKafkaConsumeInsufficientConfig
  } else if len(kafkaTopic) == 0 {
    return nil, ErrKafkaConsumeInsufficientConfig
  }

  log.Info().Msgf("Kafka brokers: %v, topic: %s", kafkaBrokersList, kafkaTopic)

  dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
    // TLS: tls.Config{...}
	}
  config := kafka.ReaderConfig{
    Brokers:          kafkaBrokersList,
    Topic:            kafkaTopic,
    MinBytes:         0,
    Dialer:           dialer,
    MaxBytes:         0,
    MaxWait:          1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
    ReadLagInterval:  0,
  }

  reader := kafka.NewReader(config)

  channel := make(chan *kafka.Message, 0)

  return &KafkaConsumer{env, config, reader, channel}, nil
}

func (b *KafkaConsumer) Close() {
  log.Warn().Msg("KakfaConsumer.Close()")
  close(b.Channel)
  b.reader.Close()
}

func (b *KafkaConsumer) Loop() {
  for {
		m, err := b.reader.ReadMessage(context.TODO())
		if err != nil {
			log.Error().Msgf("Consumer error while receiving message: %s", err.Error())
			continue
		}

		b.Channel <- &m
	}
}
