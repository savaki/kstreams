package internal

type TopicPartition struct {
	Topic     string
	Partition int32
}

type StateRestorer interface {
	OnRestoreStart()
}

type ChangelogReader struct {
	restorers []StateRestorer
}

func (c *ChangelogReader) Register(restorer StateRestorer) {
	c.restorers = append(c.restorers, restorer)
}

func (c *ChangelogReader) Restore(restorer StateRestorer) ([]TopicPartition, error) {
	return nil, nil
}

func (c *ChangelogReader) RestoredOffsets() (map[TopicPartition]int64, error) {
	return nil, nil
}
