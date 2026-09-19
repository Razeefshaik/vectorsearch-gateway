package main

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
	"github.com/segmentio/kafka-go"
)

const (
	resultOutcomeIndexed = "INDEXED"
	resultOutcomeFailed  = "FAILED"
)

type IndexResultEvent struct {
	SchemaVersion int `json:"schemaVersion"`

	CorrelationID string `json:"correlationId"`
	ClientID      string `json:"clientId"`
	VectorLabel   string `json:"vectorLabel"`

	Outcome   string `json:"outcome"`
	Duplicate bool   `json:"duplicate"`

	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`

	CompletedAt string `json:"completedAt"`
}

type ResultProducer struct {
	writer *kafka.Writer
}

func NewResultProducer(brokerAddress, topic string) *ResultProducer {
	return &ResultProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokerAddress),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

func (p *ResultProducer) Publish(
	ctx context.Context,
	ingestEvent *ingestpb.IngestEvent,
	outcome string,
	duplicate bool,
	errorCode string,
	errorMessage string,
) error {
	if ingestEvent == nil {
		return nil
	}

	correlationID := strings.TrimSpace(ingestEvent.CorrelationId)
	if correlationID == "" {
		return nil
	}

	result := IndexResultEvent{
		SchemaVersion: 1,
		CorrelationID: correlationID,
		ClientID:      strconv.FormatUint(ingestEvent.Key.ClientId, 10),
		VectorLabel:   strconv.FormatUint(ingestEvent.Key.Label, 10),
		Outcome:       outcome,
		Duplicate:     duplicate,
		ErrorCode:     sanitize(errorCode, 128),
		ErrorMessage:  sanitize(errorMessage, 1000),
		CompletedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	value, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(correlationID),
		Value: value,
		Time:  time.Now().UTC(),
	})
}

func (p *ResultProducer) Close() error {
	return p.writer.Close()
}

func sanitize(value string, maximumLength int) string {
	value = strings.TrimSpace(value)
	if len(value) <= maximumLength {
		return value
	}
	return value[:maximumLength]
}
