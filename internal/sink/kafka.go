package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// kafkaSink produces events to a Kafka topic.
//
// Message keys default to the event ID so a partitioner can keep related events
// ordered, and the canonical JSON is produced with idempotent retries enabled by
// the client, which is what makes the at-least-once contract hold through broker
// hiccups.
type kafkaSink struct {
	*Base
	client *kgo.Client
	topic  string
}

func newKafkaSink(base *Base) (Sink, error) {
	if len(base.cfg.Brokers) == 0 {
		return nil, fmt.Errorf("sink %q: at least one broker is required", base.cfg.ID)
	}
	if base.cfg.Topic == "" {
		return nil, fmt.Errorf("sink %q: topic is required", base.cfg.ID)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(base.cfg.Brokers...),
		kgo.DefaultProduceTopic(base.cfg.Topic),
		kgo.ClientID("streammesh-"+base.cfg.ID),
		kgo.ProduceRequestTimeout(10*time.Second),
		kgo.RecordDeliveryTimeout(30*time.Second),
		kgo.RecordRetries(5),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("sink %q: kafka client: %w", base.cfg.ID, err)
	}
	base.logInfo("kafka producer configured", "brokers", base.cfg.Brokers, "topic", base.cfg.Topic)
	return &kafkaSink{Base: base, client: client, topic: base.cfg.Topic}, nil
}

func (s *kafkaSink) Write(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(events))
	total := 0
	for i := range events {
		ev := events[i]
		value, err := json.Marshal(ev)
		if err != nil {
			s.noteFailure(err)
			return fmt.Errorf("sink %q: encode event: %w", s.cfg.ID, err)
		}
		total += len(value)
		records = append(records, &kgo.Record{
			Topic: s.topic,
			Key:   []byte(ev.ID),
			Value: value,
		})
	}

	results := s.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		s.noteFailure(err)
		return &RetryableError{Err: fmt.Errorf("sink %q: produce: %w", s.cfg.ID, err)}
	}

	s.noteSuccess(events, total)
	return nil
}

func (s *kafkaSink) Close(_ context.Context) error {
	// Close waits for in-flight records to be flushed.
	s.client.Close()
	return nil
}
