package kstreams

import "context"

type KafkaStreams struct {
	cancel   context.CancelFunc
	done     chan struct{}
	err      error
	topology *Topology
}

func (k *KafkaStreams) Close() error {
	k.cancel()
	<-k.done
	return k.err
}

func (k *KafkaStreams) run(ctx context.Context) {
	defer close(k.done)

	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func NewKafkaStreams(topology *Topology) *KafkaStreams {
	ctx, cancel := context.WithCancel(context.Background())

	kafkaStreams := &KafkaStreams{
		cancel:   cancel,
		done:     make(chan struct{}),
		topology: topology,
	}
	go kafkaStreams.run(ctx)

	return kafkaStreams
}
