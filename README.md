# Kafka Balancer

This tool can be used to balance partitions in a topic across brokers with minimal data movement. It is also rack aware. It also supports other features like re-replicating the under replicated partitions in balanced manner and reducing the replication factor for a topic. This tool generates/executes assignment plan based on the mode passed. If topic is not specified, this will generate/execute assignment plan for all the topics in the cluster

## Usage:
```sh
com.flipkart.fdpinfra.kafka.balancer.KafkaBalancerMain [-h] [--topic TOPIC] --zookeeper ZOOKEEPER [--rack-aware] (--balance-partitions | --balance-leaders | --balance-followers | --reassign-under-replicated-partitions | --set-replication-factor replicationFactor) (--generate | --execute)
```

## Modes:
These are the five different modes of running the balancer
1. --balance-partitions : Balance the leaders and followers for a given topic or all the topics in a Kafka cluster
2. --balance-leaders: Balance only the leaders for a given topic or all the topics in a Kafka cluster
3. --balance-followers: Balance only the followers for a given topic or all the topics in a Kafka cluster
4. --reassign-under-replicated-partitions: Re-replicate the under replicated partitions for a given topic or all the topics in a Kafka cluster in a balanced manner. Note that above balance commands will abort if there are under replicated partitions in the cluster. In that case, under replicated partitions must be cleared with this command prior
5. --set-replication-factor: Alter the replication factor for a given topic or all the topics in a Kafka cluster. Note that only reducing replication factor is supported as of now. Increasing replication factor will be supported in future versions

## Example Usages:
1. To generate a plan for balancing leader and follower partitions for all the topics in a cluster
```sh
KafkaBalancerMain --zookeeper localhost:2181/kafka-balancer-test --balance-partitions --rack-aware --generate
```

2. To execute a plan for balancing leader and follower partitions for all the topics in a cluster
```sh
KafkaBalancerMain --zookeeper localhost:2181/kafka-balancer-test --balance-partitions --rack-aware --execute
```

3. To generate a plan for balancing only follower partitions for a topic named test_topic in a cluster
```sh
KafkaBalancerMain --zookeeper localhost:2181/kafka-balancer-test --balance-followers --topic test_topic --rack-aware --generate
```

4. To generate a plan for reducing replication factor from 3 to 2 for a topic named test_topic in a cluster
```sh
KafkaBalancerMain --zookeeper localhost:2181/kafka-balancer-test --set-replication-factor 2 --topic test_topic --rack-aware --generate
```

5. To exeucte a plan for clearing off the under replicated partitions in a cluster. First one with considering rack awareness for partition placements and second one for a non-rack aware cluster 
```sh
com.flipkart.fdpinfra.kafka.balancer --zookeeper localhost:2181/kafka-balancer-test --reassign-under-replicated-partitions --rack-aware --execute

com.flipkart.fdpinfra.kafka.balancer --zookeeper localhost:2181/kafka-balancer-test --reassign-under-replicated-partitions --execute
```

## Contributors:
- Gokulakannan Muralidharan
- Ravi Teja
- Sandesh Karkera

## License

Licensed under : The Apache Software License, Version 2.0
(http://www.apache.org/licenses/LICENSE-2.0.txt)
