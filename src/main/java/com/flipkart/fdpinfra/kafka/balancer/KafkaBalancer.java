package com.flipkart.fdpinfra.kafka.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import lombok.Data;
import lombok.extern.log4j.Log4j;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.model.AscendingDebtComparator;
import com.flipkart.fdpinfra.kafka.balancer.model.AscendingLoadComparator;
import com.flipkart.fdpinfra.kafka.balancer.model.Broker;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerDebt;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerMetaData;
import com.flipkart.fdpinfra.kafka.balancer.model.DescendingDebtComparator;
import com.flipkart.fdpinfra.kafka.balancer.model.DescendingLoadComparator;
import com.flipkart.fdpinfra.kafka.balancer.model.Replica;
import com.flipkart.fdpinfra.kafka.balancer.model.UnderReplicatedPartition;
import com.flipkart.fdpinfra.kafka.balancer.utils.BalancerUtil;

/**
 * Generate partition assignment plan for balancing the partitions in a topic
 * across the cluster with minimal movements of partitions between under and
 * over utilized brokers
 * 
 * @author gokulakannan.m
 *
 */
@Data
@Log4j
public abstract class KafkaBalancer {

	public KafkaBalancer(String topic, boolean rackAware) {
		this.topic = topic;
		this.rackAware = rackAware;

		overUtilizedLeaders = new TreeSet<>(DD);
		underUtilizedLeaders = new TreeSet<>(DA);

		underReplicatedPartitions = new LinkedList<>();

		leaderAssignments = new ArrayList<>();
		followerAssignments = new ArrayList<>();
	}

	/**
	 * Abstract method must be overridden by the child class.
	 *
	 * @param partitionCount
	 *            number of partitions.
	 * @param numBrokers
	 *            number of brokers in the cluster.
	 * @throws ReplicaAssignmentException
	 */
	public abstract void generatePlan(int partitionCount, int numBrokers) throws ReplicaAssignmentException;

	/**
	 * Generates replica assignment plan.
	 *
	 * @return replica assignment plan as json string.
	 * @throws ReplicaAssignmentException
	 */
	public String generateAssignmentPlan() throws ReplicaAssignmentException {
		populateBrokerTopicDetails();
		log.info("------------------------------------------------");
		log.info("Initial state for topic " + topic + " before assignment: \n" + brokersInfo);
		int partitionCount = topicMetadata.partitionMetadata().size();
		int numBrokers = brokerMetaDataMap.size();
		log.info("partitionCount: " + partitionCount + " numBrokers: " + numBrokers + " replicationFactor: "
				+ replicationFactor);
		initializePartitionManager();
		generatePlan(partitionCount, numBrokers);
		log.debug("Plan generated. Verifying partition placements for topic " + topic + " and replicationFactor " + replicationFactor);
		validatePostAssignment(replicationFactor);
		log.debug("Partition placements verified successfully");
		if (leaderAssignments.isEmpty() && followerAssignments.isEmpty()) {
			log.info("No assignments generated for topic: " + topic);
			return null;
		} else {
			log.info("leader assignments: \n" + leaderAssignments);
			log.info("follower assignments: \n" + followerAssignments);
			String jsonOutput = BalancerUtil.convertToJsonOutput(topicMetadata, leaderAssignments, followerAssignments);
			log.info("Topic: " + topic + " State after assignment: \n" + brokersInfo);
			return jsonOutput;
		}
	}
	
	protected void validatePostAssignment(int replicationFactor) throws ReplicaAssignmentException{
		partitionManager.validatePartitionPlacements(brokersInfo, replicationFactor);
	}

	/**
	 * Populates broker and topic details.
	 */
	private void populateBrokerTopicDetails() {
		BrokersTopicsCache cache = BrokersTopicsCache.getInstance();
		brokerMetaDataMap = cache.getBrokerMetaDataMap();
		this.replicationFactor = cache.getTopicReplicationFactorMap().get(topic);
		this.topicMetadata = cache.getTopicMetadataMap().get(topic);
		populateBrokerData(topicMetadata, brokersInfo, brokerMetaDataMap);
		populateUnderReplicatedPartitions(topicMetadata);
		addRemainingBrokersInTheCluster(brokersInfo, brokerMetaDataMap);
	}

	/**
	 * Populates broker data for a given topic.
	 *
	 * @param topicMetadata
	 *            Topic metadata
	 * @param brokersInfo
	 *            <broker_id, brokerInfo> map
	 * @param brokerMetaDataMap
	 *            Broker Id - > BrokerMetaData Map
	 */
	public void populateBrokerData(TopicMetadata topicMetadata, Map<Integer, Broker> brokersInfo,
			Map<Integer, BrokerMetaData> brokerMetaDataMap) {
		// iterate through the topic metainfo
		log.debug("Topic: " + topicMetadata.topic());

		for (PartitionMetadata part : topicMetadata.partitionMetadata()) {
			String replicas = "";
			List<Integer> partitionReplicas = new ArrayList<>();

			String leaderHost = part.leader().host();
			int leaderId = part.leader().id();
			int partitionId = part.partition();

			addToBrokerList(partitionId, leaderId, brokersInfo, brokerMetaDataMap, true);
			partitionReplicas.add(leaderId);

			for (Node replica : part.replicas()) {
				int followerId = replica.id();
				replicas += " " + followerId;

				// dont add leader as replica also
				if (leaderId != followerId) {
					addToBrokerList(partitionId, followerId, brokersInfo, brokerMetaDataMap, false);
					partitionReplicas.add(followerId);
				}
			}

			log.debug("    Partition: " + partitionId + ": Leader: " + leaderHost + " Replicas:[" + replicas + "] ");
		}
	}

	/**
	 * Adds or updates broker details.
	 *
	 * @param partitionId
	 *            partition id.
	 * @param brokerId
	 *            broker id.
	 * @param leaderHost
	 *            leader host.
	 * @param brokerList
	 *            list of all brokers.
	 * @param brokerMetaDataMap
	 *            map of broker id to broker meta data
	 * @param isLeader
	 *            true if the given partition is a leader.
	 */
	private void addToBrokerList(int partitionId, Integer brokerId, Map<Integer, Broker> brokerList,
			Map<Integer, BrokerMetaData> brokerMetaDataMap, boolean isLeader) {
		Broker broker = brokerList.get(brokerId);
		if (broker != null) {
			broker.addPartition(partitionId, isLeader);
		} else {
			Broker newBroker = constructBroker(brokerId, brokerMetaDataMap);
			newBroker.addPartition(partitionId, isLeader);
			brokerList.put(brokerId, newBroker);
		}
	}

	private Broker constructBroker(Integer brokerId, Map<Integer, BrokerMetaData> brokerMetaDataMap) {
		BrokerMetaData brokerMeta = brokerMetaDataMap.get(brokerId);
		Broker newBroker = new Broker(brokerId, brokerMeta.getNumTotalLeaders(), brokerMeta.getNumTotalFollowers(),
				BrokersTopicsCache.getInstance());
		if (rackAware) {
			newBroker.setRack(brokerMeta.getRack());
		}
		return newBroker;
	}

	/**
	 * Adds broker to brokerInfos map if it is missing.
	 *
	 * @param brokerInfos
	 *            <broker_id, brokerInfo> map
	 * @param brokerMetaData
	 *            Broker details like rack information, partition count etc
	 */
	private void addRemainingBrokersInTheCluster(Map<Integer, Broker> brokerInfos,
			Map<Integer, BrokerMetaData> brokerMetaData) {
		for (Entry<Integer, BrokerMetaData> entry : brokerMetaData.entrySet()) {
			Integer brokerId = entry.getKey();
			if (!brokerInfos.containsKey(brokerId)) {
				brokerInfos.put(brokerId, constructBroker(brokerId, brokerMetaData));
			}
		}
	}

	/**
	 * Calculates debt for each broker depending on leadersPerBrokerIdealCount
	 * and followersPerBrokerIdealCount. Add the broker to corresponding debt
	 * list i.e. underUtilized/balanced/overUtilized.
	 *
	 * @param leadersPerBrokerIdealCount
	 *            total_leader_replicas/total_brokers
	 * @param followersPerBrokerIdealCount
	 *            total_follower_replicas/total_brokers
	 * @param brokerInfos
	 *            Mapping of brokerId to brokerInfo.
	 */
	protected void populateBrokerDebt(int leadersPerBrokerIdealCount, int followersPerBrokerIdealCount,
			Map<Integer, Broker> brokerInfos) {

		// add debt for existing brokers
		for (Entry<Integer, Broker> brokerInfo : brokerInfos.entrySet()) {
			Broker broker = brokerInfo.getValue();

			int leaderDebt = leadersPerBrokerIdealCount - broker.getLeaders().size();
			int followerDebt = followersPerBrokerIdealCount - broker.getFollowers().size();

			if (leaderDebt > 0) {
				underUtilizedLeaders.add(new BrokerDebt(leaderDebt, broker, true));
			} else if (leaderDebt < 0) {
				overUtilizedLeaders.add(new BrokerDebt(Math.abs(leaderDebt), broker, true));
			}

			if (followerDebt > 0) {
				underUtilizedFollowers.add(new BrokerDebt(followerDebt, broker, false));
			} else if (followerDebt < 0) {
				overUtilizedFollowers.add(new BrokerDebt(Math.abs(followerDebt), broker, false));
			} else {
				balancedFollowers.add(new BrokerDebt(Math.abs(followerDebt), broker, false));
			}
		}
	}

	/**
	 * Populates underReplicated partitions list for a given topic.
	 *
	 * @param topicMetadata
	 *            topic metadata.
	 */
	private void populateUnderReplicatedPartitions(TopicMetadata topicMetadata) {
		if (topicMetadata == null) {
			throw new UnsupportedOperationException("Cannot populate under replicated partitions since "
					+ "topic metadata is not present.");
		}

		for (PartitionMetadata part : topicMetadata.partitionMetadata()) {
			String partitionError = part.error().name();

			if ((partitionError != null) && partitionError.equals(LEADER_NOT_AVAILABLE)) {
				throw new UnsupportedOperationException(LEADER_NOT_AVAILABLE + " for " + "Topic: " + topic
						+ " partition: " + part.partition());
			}

			int underReplicationCount = replicationFactor - part.isr().size();
			if (underReplicationCount > 0) {
				underReplicatedPartitions.add(new UnderReplicatedPartition(part.partition(), underReplicationCount));
			}
		}
	}

	/**
	 * Construct and initialize the partitionManager based on rack awareness or
	 * simple mode
	 * 
	 */
	void initializePartitionManager() throws ReplicaAssignmentException{
		if (rackAware) {
			partitionManager = new RackAwarePartitionManager(brokersInfo);
		} else {
			partitionManager = new PartitionManager();
		}
	}

	// Visible for testing
	public List<Replica> getLeaderAssignments() {
		return Collections.unmodifiableList(leaderAssignments);
	}

	// Visible for testing
	public List<Replica> getFollowerAssignments() {
		return Collections.unmodifiableList(followerAssignments);
	}

	private static final String LEADER_NOT_AVAILABLE = "LEADER_NOT_AVAILABLE";
	private static final AscendingLoadComparator ASCENDING_LOAD_COMPARATOR = new AscendingLoadComparator();
	private static final DescendingLoadComparator DESCENDING_LOAD_COMPARATOR = new DescendingLoadComparator();

	protected static final DescendingDebtComparator DA = new DescendingDebtComparator(ASCENDING_LOAD_COMPARATOR);
	protected static final DescendingDebtComparator DD = new DescendingDebtComparator(DESCENDING_LOAD_COMPARATOR);
	protected static final AscendingDebtComparator AA = new AscendingDebtComparator(ASCENDING_LOAD_COMPARATOR);

	protected String topic;
	protected int replicationFactor;
	private boolean rackAware = false;

	protected Set<BrokerDebt> overUtilizedLeaders;
	protected Set<BrokerDebt> underUtilizedLeaders;

	protected Set<BrokerDebt> overUtilizedFollowers;
	protected Set<BrokerDebt> balancedFollowers;
	protected Set<BrokerDebt> underUtilizedFollowers;

	protected List<Replica> leaderAssignments;
	protected List<Replica> followerAssignments;

	protected PartitionManager partitionManager;
	protected List<UnderReplicatedPartition> underReplicatedPartitions;

	protected Map<Integer, Broker> brokersInfo = new HashMap<>();
	protected Map<Integer, BrokerMetaData> brokerMetaDataMap;

	protected TopicMetadata topicMetadata;

}
