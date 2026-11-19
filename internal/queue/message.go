package queue

type Message struct {
	Topic string
	Key   string
	Value string

	// Partition and Offset identify the record on the consuming side;
	// they are zero for messages built for producing.
	Partition int32
	Offset    int64
}

func (m Message) GetTopic() string {
	return m.Topic
}

// GetKey returns nil for an empty key. A non-nil empty slice would be treated by
// the Kafka client as a real key, hashed, and every keyless message would land
// in the same partition.
func (m Message) GetKey() []byte {
	if m.Key == "" {
		return nil
	}

	return []byte(m.Key)
}

func (m Message) GetValue() []byte {
	return []byte(m.Value)
}
