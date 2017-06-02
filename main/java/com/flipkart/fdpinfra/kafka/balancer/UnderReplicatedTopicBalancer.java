package com.flipkart.fdpinfra.kafka.balancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import lombok.extern.log4j.Log4j;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerDebt;
import com.flipkart.fdpinfra.kafka.balancer.model.Replica;
import com.flipkart.fdpinfra.kafka.balancer.model.UnderReplicatedPartition;

@Log4j
public class UnderReplicatedTopicBalancer extends KafkaBalancer {
	public UnderReplicatedTopicBalancer(String topic, boolean rackAware) {
		super(topic, rackAware);
		underUtilizedFollowers = new TreeSet<>(DA);
		balancedFollowers = new TreeSet<>(DA);
		overUtilizedFollowers = new TreeSet<>(AA);
	}

	private enum Utilization {
		UNDER, BALANCED, OVER;
	}

	/**
	 * Generates partition assignment plan. Does the initial calculations
	 * required before proceeding with the reassignment and calls reassignment
	 * logic
	 *
	 * @param partitionCount
	 *            number of partitions.
	 * @param numBrokers
	 *            number of brokers in the cluster.
	 * 
	 * @throws ReplicaAssignmentException
	 */
	@Override
	public void generatePlan(int partitionCount, int numBrokers) throws ReplicaAssignmentException {
		if (underReplicatedPartitions.isEmpty()) {
			log.debug("No under replicated partitions found to reassign for topic: " + topic + ".");
			return;
		}
		log.info("Under replicated partitions: \n" + underReplicatedPartitions);
		int followersTotalCount = partitionCount * (replicationFactor - 1);
		int followersPerBrokerIdealCount = followersTotalCount / numBrokers;
		log.info("FollowersPerBrokerIdealCount:" + followersPerBrokerIdealCount);
		populateBrokerDebt(0, followersPerBrokerIdealCount, brokersInfo);
		log.info("Utilization: \n" + "Under utilized followers: " + underUtilizedFollowers + "\nBalanced followers: "
				+ balancedFollowers + "\nOver utilized followers: " + overUtilizedFollowers);

		reassignUnderreplicatedPartitions(followerAssignments);

		if (!underReplicatedPartitions.isEmpty()) {
			throw new ReplicaAssignmentException("Could not reassign under replicated partitions : "
					+ underReplicatedPartitions);
		}
	}

	/**
	 * Reassigns under replicated partitions to brokers. Reassignment logic is
	 * as follows: 1. For each under replicated partition If under utilized
	 * brokers exist Try to assign replicas to under utilized brokers else Try
	 * to assign replicas to balanced brokers else Try to assign replicas to
	 * overutilized brokers.
	 *
	 * if partition is still under-replicated throw exception
	 *
	 * @param reassignedURP
	 *            List of reassigned replicas.
	 * @throws ReplicaAssignmentException
	 */
	private void reassignUnderreplicatedPartitions(List<Replica> reassignedURP) throws ReplicaAssignmentException {
		if (underReplicatedPartitions.isEmpty())
			return;

		Iterator<UnderReplicatedPartition> it = underReplicatedPartitions.iterator();
		while (it.hasNext()) {
			UnderReplicatedPartition part = it.next();
			Collection<BrokerDebt> assigned = null;
			if (!underUtilizedFollowers.isEmpty()) {
				assigned = reassignPartitionsToFollowers(part, reassignedURP, underUtilizedFollowers, Utilization.UNDER);
				addAll(assigned, underUtilizedFollowers);
			}
			if (part.getReplicaDebt() > 0 && !balancedFollowers.isEmpty()) {
				assigned = reassignPartitionsToFollowers(part, reassignedURP, balancedFollowers, Utilization.BALANCED);
				addAll(assigned, balancedFollowers);
			}
			if (part.getReplicaDebt() > 0 && !overUtilizedFollowers.isEmpty()) {
				assigned = reassignPartitionsToFollowers(part, reassignedURP, overUtilizedFollowers, Utilization.OVER);
				addAll(assigned, overUtilizedFollowers);
			}

			if (part.getReplicaDebt() == 0) {
				it.remove();
			} else {
				throw new ReplicaAssignmentException("Could not reassign under replicated partition: " + topic + "-"
						+ part);
			}
		}
	}

	private void addAll(Collection<BrokerDebt> from, Collection<BrokerDebt> to) {
		if (from != null && !from.isEmpty()) {
			to.addAll(from);
		}
	}

	/**
	 * Logic: 1. For each broker in brokerDebts list if broker and partition are
	 * on different rack assign replica to the broker update the number of
	 * under-replicated replicas update partitionToRacks mapping update the
	 * broker info -> add partition to broker. add broker to assigned list
	 * update the broker debt and move it to correct category.
	 *
	 * if under replicated replicas for partition is 0, remove it from
	 * under-replicated partitions list.
	 *
	 * return assigned brokers.
	 *
	 * @param urp
	 *            under-replicated partition.
	 * @param reassignURP
	 *            list of reassigned replicas.
	 * @param brokerDebts
	 *            list of candidate brokers.
	 * @param utilization
	 *            type of candidate brokers.
	 * @return list of brokers to which the partitions were reassigned.
	 */

	private Collection<BrokerDebt> reassignPartitionsToFollowers(UnderReplicatedPartition urp,
			List<Replica> reassignURP, Set<BrokerDebt> brokerDebts, Utilization utilization) throws ReplicaAssignmentException{
		Iterator<BrokerDebt> it = brokerDebts.iterator();
		Collection<BrokerDebt> assigned = new ArrayList<>();
		while (it.hasNext()) {
			BrokerDebt b = it.next();
			if (partitionManager.validateCandidate(null, b, urp.getPartition())) {
				reassignURP.add(new Replica(urp.getPartition(), false, -1, b.getBroker().getId()));
				urp.setReplicaDebt(urp.getReplicaDebt() - 1);
				partitionManager.addPartition(b.getBroker(), false, urp.getPartition());
				assigned.add(b);

				switch (utilization) {
				case UNDER:
					b.setDebt(b.getDebt() - 1);
					if (b.getDebt() == 0) {
						balancedFollowers.add(b);
						assigned.remove(b);
					}
					break;
				case BALANCED:
					b.setDebt(b.getDebt() + 1);
					if (b.getDebt() > 0) {
						overUtilizedFollowers.add(b);
						assigned.remove(b);
					}
					break;
				case OVER:
					b.setDebt(b.getDebt() + 1);
					break;
				}

				it.remove();
			}
			if (urp.getReplicaDebt() == 0)
				break;
		}

		return assigned;
	}
}
