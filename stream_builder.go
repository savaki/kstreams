package kstreams

type StreamBuilder struct {
}

func (s *StreamBuilder) GlobalTable(topic string) {
	requireString(topic, "topic MUST NOT be blank")


}
