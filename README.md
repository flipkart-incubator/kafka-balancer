Kafka Balancer

This tool is used to balance partitions for topic(s) across brokers with minimal data movement. It is also rack aware.

Usage: KafkaBalancer [-h] [--topic TOPIC] --zookeeper ZOOKEEPER [--rack-aware] (--balance-partitions | --balance-leaders | --balance-followers | --reassign-under-replicated-partitions | --set-replication-factor replicationFactor) (--generate | --execute)
