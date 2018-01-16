package kstreams

// ProcessorContext interface
type ProcessorContext interface {
	// ApplicationID returns the application id
	ApplicationID() string

	// TaskID returns the task id
	TaskID() string

	// Forwards a key, value pair to the downstream processors
	Forward(key, value Encoder) error

	// Commit request
	Commit()

	// Topic name of the current input record; could be null if it is not
	// available (for example, if this method is invoked from the punctuate call)
	Topic() string

	// Partition id of the current input record; could be -1 if it is not
	// available (for example, if this method is invoked from the punctuate call)
	Partition() int32

	// Offset of the current input record; could be -1 if it is not
	// available (for example, if this method is invoked from the punctuate call)
	Offset() int64

	// StateStore retrieves the StateStore with the specified name
	StateStore(name string) (StateStore, bool)
}

// Initializer initializes this processor with the given context. The
// framework ensures this is called once per processor when the Topology
// that contains it is initialized.
type Initializer interface {
	// Init the process.  The framework ensures this will only be called once
	// when the Topology is initialized
	Init(pc ProcessorContext) error
}

// Closer tears down any resources used by the Processor
type Closer interface {
	// Close any resources in use by the Processor
	Close() error
}

// Processor of key value pair records.
//
// If the Processor needs to be initialized prior to execute, the Processor MUST
// implement kstreams.Initializer.
//
// If the processor needs to clean up resources on teardown, the Processor MUST
// implement kstreams.Closer
//
type Processor interface {
	// Process the record with the given key and value
	Process(pc ProcessorContext, kv KeyValue) error
}

// ProcessorFunc provides a func type to create a processor
type ProcessorFunc func(pc ProcessorContext, kv KeyValue) error

// Process implements Processor
func (fn ProcessorFunc) Process(pc ProcessorContext, kv KeyValue) error {
	return fn(pc, kv)
}

// ProcessorSupplier is a factory method that instantiates Processors
type ProcessorSupplier interface {
	// Get returns new instance
	New() Processor
}

// ProcessorSupplierFunc provides a func wrapper for ProcessorSupplier
type ProcessorSupplierFunc func() ProcessorFunc

// New implements ProcessorSupplier
func (fn ProcessorSupplierFunc) New() Processor {
	return fn()
}
