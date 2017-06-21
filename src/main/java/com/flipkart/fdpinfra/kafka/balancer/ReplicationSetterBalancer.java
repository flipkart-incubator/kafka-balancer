package com.flipkart.fdpinfra.kafka.balancer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.model.Replica;

public class ReplicationSetterBalancer extends KafkaBalancer {

	private static final boolean IS_LEADER = false;
	private static final int MIN_REPLICATION_FACTOR = 2;
	private int newReplicationFactor;
	private TopicMetadata tm;

	public ReplicationSetterBalancer(String topic, boolean rackAware, int newReplicationFactor) {
		super(topic, rackAware);
		this.newReplicationFactor = newReplicationFactor;
	}

	@Override
	public void generatePlan(int partitionCount, int numBrokers) throws ReplicaAssignmentException {
		validatePreAssignment();
		setPartitionMetadata();
		int currentReplicationFactor = replicationFactor;
		while (currentReplicationFactor - newReplicationFactor > 0) {
			for (PartitionMetadata partitionMetadata : tm.partitionMetadata()) {
				List<Node> replicas = partitionMetadata.replicas();
				int partition = partitionMetadata.partition();
				int brokerId = getTargetBroker(replicas, partitionMetadata.leader().id());
				followerAssignments.add(new Replica(partition, IS_LEADER, brokerId, -1));
				brokersInfo.get(brokerId).removePartition(partition, IS_LEADER);
			}
			currentReplicationFactor--;
		}
	}

	private void setPartitionMetadata() {
		List<PartitionMetadata> clonePartitionMetadata = new ArrayList<>();
		TopicMetadata cloneTopicMetadata = new TopicMetadata(topicMetadata.error(), topicMetadata.topic(),
				topicMetadata.isInternal(), clonePartitionMetadata);
		for (PartitionMetadata p : topicMetadata.partitionMetadata()) {
			clonePartitionMetadata.add(new PartitionMetadata(p.error(), p.partition(), p.leader(), new ArrayList<>(p
					.replicas()), p.isr()));
		}
		this.tm = cloneTopicMetadata;
	}

	@Override
	protected void validatePostAssignment(int replicationFactor) throws ReplicaAssignmentException {
		super.validatePostAssignment(newReplicationFactor);
	}

	private int getTargetBroker(List<Node> replicas, int leader) {
		int index = 0;
		int mostLoaded = Integer.MIN_VALUE;
		int size = replicas.size();
		for (int i = size - 1; i >= 0; i--) {
			Node node = replicas.get(i);
			int id = node.id();
			if (id == leader) {
				continue;
			}
			Integer numFollowers = brokersInfo.get(id).getNumFollowers();
			if (mostLoaded < numFollowers) {
				mostLoaded = numFollowers;
				index = i;
			}
		}
		return replicas.remove(index).id();
	}

	private void validatePreAssignment() throws ReplicaAssignmentException {
		if (!underReplicatedPartitions.isEmpty()) {
			throw new UnsupportedOperationException(
					"Under replicated partitions found for the topic, clear them off before trying to balance: "
							+ underReplicatedPartitions);
		}

		partitionManager.validatePartitionPlacements(brokersInfo, replicationFactor);

		if (newReplicationFactor > replicationFactor) {
			throw new UnsupportedOperationException(
					"Increasing replication factor is not supported right now: Current: " + replicationFactor
							+ ", Requested:" + newReplicationFactor);
		}

		if (newReplicationFactor < MIN_REPLICATION_FACTOR) {
			throw new IllegalArgumentException("Replication factor : " + newReplicationFactor + " cannot be less than "
					+ MIN_REPLICATION_FACTOR);
		}
	}
}