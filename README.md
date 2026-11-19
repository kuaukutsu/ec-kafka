# kafka workflow

Стенд разворачивается отдельно, на базе `apache/kafka:4.3.2`.

Три ноды, с настройкой по умолчанию:
```env
KAFKA_NUM_PARTITIONS: 6
KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_MIN_INSYNC_REPLICAS: 2
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

## go stack

- https://github.com/twmb/franz-go
