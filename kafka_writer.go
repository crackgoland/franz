package franz

import (
  "time"
  "context"
  "strings"
  "errors"
  "github.com/rs/zerolog/log"
  "github.com/segmentio/kafka-go"
  "github.com/crackgoland/env"
  "github.com/segmentio/kafka-go/snappy"
)

var (
  ErrKafkaWriterInsufficientConfig = errors.New("Not enough data specified for KafkaWriter in environment")
)

type KafkaWriter struct {
  env     *env.Env
  config  kafka.WriterConfig
  writer  *kafka.Writer
  Channel chan kafka.Message
}

func NewKafkaWriter(env *env.Env) (*KafkaWriter, error) {

  kafkaBrokers, _ := env.Get("KAFKA_BROKERS", "kafka1:9092")
  kafkaTopic, _ := env.Get("KAFKA_TOPIC", "changeme")

  kafkaBrokersList := strings.Split(kafkaBrokers, ",")

  if len(kafkaBrokersList) == 0 {
    return nil, ErrKafkaWriterInsufficientConfig
  } else if len(kafkaTopic) == 0 {
    return nil, ErrKafkaWriterInsufficientConfig
  }

  log.Info().Msgf("Kafka brokers: %v, topic: %s", kafkaBrokersList, kafkaTopic)

  dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
    // TLS: tls.Config{...}
	}
	config := kafka.WriterConfig{
		Brokers:          kafkaBrokersList,
		Topic:            kafkaTopic,
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
	}

  writer := kafka.NewWriter(config)

  channel := make(chan kafka.Message, 0)

  return &KafkaWriter{env, config, writer, channel}, nil
}

func (b *KafkaWriter) Close() {
  log.Warn().Msg("KakfaWriter.Close()")
  close(b.Channel)
  b.writer.Close()
}

func (b *KafkaWriter) Loop() {
  for {
		m, more := <- b.Channel
    if !more {
			log.Error().Msgf("KafkaWriter.Loop() stopped: no more messages")
      return
    }

    err := b.writer.WriteMessages(context.TODO(), m)
		if err != nil {
			log.Error().Msgf("Writer error while write message: %s", err.Error())
			continue
		}
	}
}

func StringMessage(value string) kafka.Message {
  return kafka.Message{
    Key:        nil,
    Value:      []byte(value),
    Time:       time.Now().UTC(),
  }
}

func Message(value []byte) kafka.Message {
  return kafka.Message{
    Key:        nil,
    Value:      value,
    Time:       time.Now(),
  }
}
