package com.flipkart.fdpinfra.kafka.balancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerDebt;
import com.flipkart.fdpinfra.kafka.balancer.model.Replica;

@Log4j
public class TopicBalancer extends KafkaBalancer {

	public static final String BALANCE_LEADERS = "BalanceLeaders";
	public static final String BALANCE_FOLLOWERS = "BalanceFollowers";

	private boolean balanceLeadersOnly;
	private boolean balanceFollowersOnly;

	public TopicBalancer(String topic, boolean rackAware, String task) {
		super(topic, rackAware);
		overUtilizedFollowers = new TreeSet<>(DD);
		balancedFollowers = new TreeSet<>(DD);
		underUtilizedFollowers = new TreeSet<>(DA);

		if (BALANCE_LEADERS.equals(task)) {
			balanceLeadersOnly = true;
		} else if (BALANCE_FOLLOWERS.equals(task)) {
			balanceFollowersOnly = true;
		}
	}

	/**
	 * Generates partition reassignment plan. Populates the required info and
	 * calls leader and follower reassignment methods.
	 *
	 * @param partitionCount
	 *            number of partitions.
	 * @param numBrokers
	 *            number of brokers in the cluster.
	 */
	@Override
	public void generatePlan(int partitionCount, int numBrokers) throws ReplicaAssignmentException {
		if (!underReplicatedPartitions.isEmpty()) {
			throw new UnsupportedOperationException(
					"Under replicated partitions found for the topic, clear them off before trying to balance: "
							+ underReplicatedPartitions);
		}

		if (partitionCount < numBrokers) {
			log.info("Not proceeding with generating balancer assignement for " + topic + " since partitionCount "
					+ partitionCount + " is less then number of brokers " + numBrokers);
			return;
		}

		partitionManager.validatePartitionPlacements(brokersInfo, replicationFactor);

		int leadersPerBrokerIdealCount = partitionCount / numBrokers;
		int followersTotalCount = partitionCount * (replicationFactor - 1);
		int followersPerBrokerIdealCount = followersTotalCount / numBrokers;

		log.info(" leadersPerBrokerIdealCount: " + leadersPerBrokerIdealCount + " followersPerBrokerIdealCount:"
				+ followersPerBrokerIdealCount);

		populateBrokerDebt(leadersPerBrokerIdealCount, followersPerBrokerIdealCount, brokersInfo);

		if (!balanceFollowersOnly) {
			log.info("Utilization: \n" + "Under utilized leaders: " + underUtilizedLeaders
					+ "\nOver utilized leaders: " + overUtilizedLeaders);
			leaderAssignments = generateAssignments(underUtilizedLeaders, overUtilizedLeaders, true);
			validateDebtsPostAssignment(underUtilizedLeaders, "Under utilized leaders");
		}

		if (!balanceLeadersOnly) {
			log.info("Utilization: \n" + "Under utilized followers: " + underUtilizedFollowers
					+ "\nOver utilized followers: " + overUtilizedFollowers);
			followerAssignments = generateAssignments(underUtilizedFollowers, overUtilizedFollowers, false);
			validateDebtsPostAssignment(underUtilizedFollowers, "Under utilized followers");
		}
	}

	/**
	 * Prints the list of brokerDebt where debt is greater than 0.
	 *
	 * @param potentialDebts
	 *            list of brokerDebt.
	 * @param description
	 *            string description to be logged.
	 */
	private void validateDebtsPostAssignment(Collection<BrokerDebt> potentialDebts, String description) {
		List<BrokerDebt> actualDebts = potentialDebts.stream().filter(item -> item.getDebt() > 0)
				.collect(Collectors.toList());
		if (!actualDebts.isEmpty()) {
			log.warn(description + " found even after assignment: " + actualDebts);
		}
	}

	/**
	 * For each dst debts try to move replicas from src debts to make the
	 * cluster balanced
	 * 
	 * @param srcDebts
	 * @param dstDebts
	 *
	 * @param idealLeaderCountPerBroker
	 *            ideal number of leaders per broker.
	 * @return list of reassigned leader replicas.
	 */
	private List<Replica> generateAssignments(Set<BrokerDebt> dstDebts, Set<BrokerDebt> srcDebts, boolean isLeader)
			throws ReplicaAssignmentException {
		List<Replica> assignments = new ArrayList<>();
		for (BrokerDebt dst : dstDebts) {
			Iterator<BrokerDebt> iterator = srcDebts.iterator();
			while (iterator.hasNext()) {
				BrokerDebt src = iterator.next();
				if (src.getDebt() > 0) {
					List<Replica> candidatePartitions = partitionManager.selectCandidatePartitions(dst, src, isLeader);
					if (!candidatePartitions.isEmpty()) {
						assignments.addAll(candidatePartitions);
					}
				} else {
					iterator.remove();
				}
				if (dst.getDebt() == 0) {
					break;
				}
			}

		}
		return assignments;
	}

}
