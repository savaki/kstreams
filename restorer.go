package kstreams

// TopicPartition holds the canonical Kafka topic partition pair
type TopicPartition struct {
	// Topic reference
	Topic string

	// Partition reference
	Partition int32
}

// Record defines a low level description of a key value pair
type Record struct {
	// Key of record
	Key []byte

	// Value of record
	Value []byte
}

// StateRestorer restores current state to a KeyValueStore
type StateRestorer interface {
	// Restore restores a number of records.  This func is called repeatedly
	// until the StateStore is fully restored.
	Restore(records ...Record) error
}

type StateRestorerStarter interface {
	// OnRestoreStart will be called when the restoration process starts
	OnRestoreStart(tp TopicPartition, storeName string, startingOffset, endingOffset int64)
}

type StateRestorerEnder interface {
	// OnRestoreEnd will be called when restoration is ends
	OnRestoreEnd(tp TopicPartition, storeName string, totalRestored int64)
}
