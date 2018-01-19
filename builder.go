package kstreams

import (
	"fmt"
	"sync/atomic"
)

const (
	prefixSourceName = "KSTREAM-SOURCE-"
	prefixSinkName   = "KSTREAM-SINK-"
)

type Stream struct {
	builder     *Builder
	name        string
	repartition bool
}

func (s *Stream) To(topic string) {
	requireString(topic, "topic MUST NOT be blank")

	name := s.builder.newProcessorName(prefixSinkName)
	s.builder.topology.AddSource(name, topic)
}

func newStream(b *Builder, name string, repartition bool) *Stream {
	return &Stream{
		builder:     b,
		name:        name,
		repartition: repartition,
	}
}

type Builder struct {
	topology Topology
	index    int32
	err      error
}

func (b *Builder) newProcessorName(prefix string) string {
	return fmt.Sprintf("%v%010d", prefix, atomic.AddInt32(&b.index, 1))
}

func (b *Builder) Stream(topics ...string) *Stream {
	requireStringArray(topics, "topic MUST NOT be blank")

	name := b.newProcessorName(prefixSourceName)
	b.topology.AddSource(name, topics...)

	return newStream(b, name, false)
}

func (b *Builder) Build() *Topology {
	return &Topology{}
}

func NewBuilder() *Builder {
	return &Builder{}
}
