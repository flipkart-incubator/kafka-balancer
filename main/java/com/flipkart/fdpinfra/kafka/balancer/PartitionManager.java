package com.flipkart.fdpinfra.kafka.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.model.Broker;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerDebt;
import com.flipkart.fdpinfra.kafka.balancer.model.Replica;
import lombok.extern.log4j.Log4j;

@Log4j
public class PartitionManager {

	/**
	 * Generates a list of candidate replicas that can be moved from src to dst
	 * broker.
	 *
	 * @param dst
	 *            Destination broker.
	 * @param src
	 *            Source broker.
	 * @param isLeader
	 *            true if moving leader.
	 * @return list of replicas that can be moved from source to broker.
	 */
	public List<Replica> selectCandidatePartitions(BrokerDebt dst, BrokerDebt src, boolean isLeader) throws ReplicaAssignmentException{
		int srcDebt = src.getDebt();

		if (srcDebt == 0) {
			return null;
		}

		List<Replica> partitions = new ArrayList<>();
		List<Integer> srcPartitions = new ArrayList<>(src.getBroker().getPartitions(isLeader));
		int numCandidatePartitions = Math.min(srcDebt, dst.getDebt());
		Collections.reverse(srcPartitions);

		Iterator<Integer> srcPartitionsIterator = srcPartitions.iterator();

		while (numCandidatePartitions > 0 && srcPartitionsIterator.hasNext()) {
			int candidatePartition = srcPartitionsIterator.next();
			if (validateCandidate(src, dst, candidatePartition)) {
				partitions.add(new Replica(candidatePartition, isLeader, src.getBroker().getId(), dst.getBroker()
						.getId()));
				removePartition(src.getBroker(), isLeader, candidatePartition);
				addPartition(dst.getBroker(), isLeader, candidatePartition);
				numCandidatePartitions--;
			}
		}

		if (partitions.size() > 0) {
			src.setDebt(srcDebt - partitions.size());
			dst.setDebt(dst.getDebt() - partitions.size());
		}

		return partitions;
	}

	protected void removePartition(Broker b, boolean isLeader, int partition) {
		b.removePartition(partition, isLeader);
	}

	protected void addPartition(Broker b, boolean isLeader, int partition) throws ReplicaAssignmentException{
		b.addPartition(partition, isLeader);
	}

	/**
	 * Validates if a partition can be moved from src to dst.
	 *
	 * @param src
	 *            source broker.
	 * @param dst
	 *            destination broker.
	 * @param candidatePartition
	 *            partition id.
	 * @return true if the replica can be reassigned.
	 */
	public boolean validateCandidate(BrokerDebt src, BrokerDebt dst, int candidatePartition) {
		return !dst.getBroker().hasPartition(candidatePartition);
	}

	/**
	 * Validates that a partition replica does not lie on the same broker.
	 * @param brokerInfos
	 * 			  BrokerId to broker details map.
	 * @param replicationFactor
	 * 			  Replication factor for the topic.
	 * @throws ReplicaAssignmentException
	 */
	public void validatePartitionPlacements(Map<Integer, Broker> brokerInfos, int replicationFactor) throws ReplicaAssignmentException{
		Map<Integer, Set<Integer>> partitionToBrokersMap = populatePartitionToBrokersMap(brokerInfos);
		for (Integer partitionId : partitionToBrokersMap.keySet())
		{
			log.debug("Partition id: " + partitionId + ",Brokers: " + partitionToBrokersMap.get(partitionId));
			if (partitionToBrokersMap.get(partitionId).size() != replicationFactor)
				throw new ReplicaAssignmentException("Invalid partition placement detected. Partition:" + partitionId
						+ " Brokers: " + partitionToBrokersMap.get(partitionId) + " not equal to replicationFactor: "
						+ replicationFactor);
		}
	}

	/**
	 * Populates partition to broker mapping.
	 *
	 * @param brokerInfos
	 * 			BrokerId to broker details map.
	 * @return
	 * 			Partition to brokers map.
	 * @throws ReplicaAssignmentException
	 */
	private Map<Integer, Set<Integer>> populatePartitionToBrokersMap (Map<Integer, Broker> brokerInfos) throws ReplicaAssignmentException {

		Map<Integer, Set<Integer>> partitionToBrokersMap = new HashMap<>();

		for (Map.Entry<Integer, Broker> entry : brokerInfos.entrySet()) {
			Broker broker = entry.getValue();
			Integer brokerId = broker.getId();
			List<Integer> partitions = broker.getPartitions();
			for (Integer partition : partitions) {
				Set<Integer> brokerIds;
				if (partitionToBrokersMap.containsKey(partition)) {
					brokerIds = partitionToBrokersMap.get(partition);
				} else {
					brokerIds = new HashSet<>();
					partitionToBrokersMap.put(partition, brokerIds);
				}
				if (!brokerIds.add(brokerId))
					throw new ReplicaAssignmentException("Replica assignment failed. Broker " + brokerId +
							" already contains replica for partition " + partition);
			}
		}

		return partitionToBrokersMap;
	}

}
