package kstreams

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

type nodeFactory interface {
	// Name of the node
	Name() string

	// Build instantiates the processor
	Build() Processor
}

// Topology provides a logical representation of a Processor Topology.  A
// topology is an acyclic graph of sources, processors, and sinks.
//
// A SourceNode is a node in the graph that consumes one or more Kafka
// topics and forwards them to its successor nodes.
//
// A Processor is a node in the graph that receives input records from
// upstream nodes, processes the records, and optionally forwarding new
// records to one or all of its downstream nodes.
//
// Finally, a SinkNode is a node in the graph that receives records from
// upstream nodes and writes them to a Kafka topic.
//
// A Topology allows you to construct an acyclic graph of these nodes, and then
// passed into a new KafkaStreams instance that will then KafkaStreams.Start()
// begin consuming, processing, and producing records.
type Topology struct {
	mutex            sync.Mutex
	sourceTopicNames map[string]struct{}
	globalTopicNames map[string]struct{}
	nodeFactories    []nodeFactory
}

func (t *Topology) verifyTopicNotAlreadyRegistered(topic string) {
	// validate
	if _, ok := t.sourceTopicNames[topic]; ok {
		panic(fmt.Errorf("topic, %v, already registered by another source", topic))
	}
	if _, ok := t.globalTopicNames[topic]; ok {
		panic(fmt.Errorf("topic, %v, already registered by another source", topic))
	}
}

func (t *Topology) nodeFactoryContains(name string) bool {
	for _, nodeFactory := range t.nodeFactories {
		if nodeFactory.Name() == name {
			return true
		}
	}

	return false
}

func (t *Topology) AddSource(name string, topics ...string) error {
	// validate
	requireString(name, "name MUST NOT be blank")
	requireStringArray(topics, "topic MUST NOT be blank")

	if exists := t.nodeFactoryContains(name); exists {
		return errors.Errorf("processor, %v, is already added", name)
	}
	for _, topic := range topics {
		if topic == "" {
			return errors.Errorf("topic name may not be blank")
		}
		if err := t.verifyTopicNotAlreadyRegistered(topic); err != nil {
			return err
		}
	}

	for _, topic := range topics {
		t.sourceTopicNames[topic] = struct{}{}
	}

	return errors.New("not implemented")
}

func (t *Topology) AddSink(name string, topics ...string) error {
	return nil
}

func NewTopology() *Topology {
	return &Topology{}
}
