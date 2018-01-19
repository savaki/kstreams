package kstreams

import (
	"testing"
)

func TestBuildPipe(t *testing.T) {
	builder := NewBuilder()
	builder.
		Stream("input").
		To("output")

	kafkaStreams := NewKafkaStreams(builder.Build())
	defer kafkaStreams.Close()
}
