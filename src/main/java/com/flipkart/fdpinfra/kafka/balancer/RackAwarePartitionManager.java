package com.flipkart.fdpinfra.kafka.balancer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import lombok.Data;
import lombok.EqualsAndHashCode;

import com.flipkart.fdpinfra.kafka.balancer.model.Broker;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerDebt;
import lombok.extern.log4j.Log4j;

@Log4j
@Data
@EqualsAndHashCode(callSuper = false)
public class RackAwarePartitionManager extends PartitionManager {
	private Map<Integer, Set<String>> partitionToRacks;

	public RackAwarePartitionManager(Map<Integer, Broker> brokerInfos) throws ReplicaAssignmentException{
		partitionToRacks = populatePartitionToRackMap(brokerInfos);
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
	@Override
	public boolean validateCandidate(BrokerDebt src, BrokerDebt dst, int candidatePartition) {
		return super.validateCandidate(src, dst, candidatePartition)
				&& validateRackPlacement(src, dst, candidatePartition);
	}

	/**
	 * Populates partition to racks map.
	 *
	 * @param brokerInfos
	 *            list of broker_id to broker mapping.
	 */
	private Map<Integer, Set<String>> populatePartitionToRackMap(Map<Integer, Broker> brokerInfos) throws ReplicaAssignmentException{
		Map<Integer, Set<String>> partitionToRacks = new HashMap<>();

		for (Entry<Integer, Broker> entry : brokerInfos.entrySet()) {
			Broker broker = entry.getValue();
			String rack = broker.getRack();
			List<Integer> partitions = broker.getPartitions();
			for (Integer partition : partitions) {
				Set<String> racks;
				if (partitionToRacks.containsKey(partition)) {
					racks = partitionToRacks.get(partition);
				} else {
					racks = new HashSet<>();
					partitionToRacks.put(partition, racks);
				}
				addToRacks(racks, rack, partition);
			}
		}
		return partitionToRacks;
	}

	/**
	 * Validates that the rack does not already contain a partition replica and
	 * that the src and dst are on different racks.
	 *
	 * @param src
	 *            source broker.
	 * @param dst
	 *            destination broker.
	 * @param partition
	 *            partition id.
	 * @return true if src and dst are on different racks and the rack does not
	 *         already contain the replica.
	 */
	private boolean validateRackPlacement(BrokerDebt src, BrokerDebt dst, int partition) {
		return isSameRackMovement(src, dst) || !isAnyOtherReplicaOnSelectedRack(dst.getBroker().getRack(), partition);
	}

	/**
	 * Validates that the src and dst brokers are on same rack.
	 *
	 * @param src
	 *            source broker.
	 * @param dst
	 *            destination broker.
	 * @return true if src and dst are on same rack.
	 */
	private boolean isSameRackMovement(BrokerDebt src, BrokerDebt dst) {
		return src != null && src.getBroker().getRack().equals(dst.getBroker().getRack());
	}

	/**
	 * Validates if the rack already contains a replica of the partition.
	 *
	 * @param rack
	 *            rack id.
	 * @param partition
	 *            partition id.
	 * @return true if the rack does not contain replica of the partition.
	 */
	private boolean isAnyOtherReplicaOnSelectedRack(String rack, int partition) {
		return getRacks(partition).contains(rack);
	}

	/**
	 * Adds rack to rackList provided. Throws exception if rack is already present.
	 *
	 * @param racks
	 * 			 Rack list.
	 * @param rack
	 *			 Rack to be added to list.
	 * @throws ReplicaAssignmentException
	 */
	private void addToRacks(Set<String> racks, String rack, int partition) throws ReplicaAssignmentException {
		if (!racks.add(rack))
			throw new ReplicaAssignmentException("Replica assignment failed since replica for partition:" + partition +
					" already exists on rack " + rack);
	}

	/**
	 * Validates partition placements. Throws error if number of racks for a partition is not equal to replicationFactor.
	 *
	 * @param brokerInfos
	 * 			 BrokerId to broker details mapping.
	 * @param replicationFactor
	 * 			 Replication factor for the topic.
	 * @throws ReplicaAssignmentException
	 */
	@Override
	public void validatePartitionPlacements(Map<Integer, Broker> brokerInfos, int replicationFactor) throws ReplicaAssignmentException{
		super.validatePartitionPlacements(brokerInfos,replicationFactor);
		Map<Integer, Set<String>> partitionToRacks = populatePartitionToRackMap(brokerInfos);
		for (Integer partitionId : partitionToRacks.keySet()) {
			log.debug("Partition id: " + partitionId + ",Racks: " + partitionToRacks.get(partitionId));
			if (partitionToRacks.get(partitionId).size() != replicationFactor)
				throw new ReplicaAssignmentException("Invalid partition placement detected. Partition:" + partitionId
						+ " Racks: " + partitionToRacks.get(partitionId) + " not equal to replicationFactor: "
						+ replicationFactor);
		}
	}

	@Override
	protected void removePartition(Broker b, boolean isLeader, int partition) {
		super.removePartition(b, isLeader, partition);
		Set<String> racks = getRacks(partition);
		Iterator<String> iterator = racks.iterator();
		while (iterator.hasNext()) {
			String rack = iterator.next();
			if (b.getRack().equals(rack)) {
				iterator.remove();
				break;
			}
		}
	}

	@Override
	protected void addPartition(Broker b, boolean isLeader, int partition) throws ReplicaAssignmentException{
		super.addPartition(b, isLeader, partition);
		Set<String> racks = getRacks(partition);
		addToRacks(racks,b.getRack(), partition);
	}

	private Set<String> getRacks(int partition) {
		return partitionToRacks.get(partition);
	}
}
