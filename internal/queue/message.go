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

func (m Message) GetKey() []byte {
	return []byte(m.Key)
}

func (m Message) GetValue() []byte {
	return []byte(m.Value)
}
