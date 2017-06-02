package com.flipkart.fdpinfra.kafka.balancer;

import static com.flipkart.fdpinfra.kafka.balancer.utils.ScalaConversions.javaCollectionToScalaSeq;
import static com.flipkart.fdpinfra.kafka.balancer.utils.ScalaConversions.scalaMapToJavaMap;
import static com.flipkart.fdpinfra.kafka.balancer.utils.ScalaConversions.scalaSeqToJavaList;
import static com.flipkart.fdpinfra.kafka.balancer.utils.ZKUtil.ZK_CONNECTION_TIMEOUT;
import static com.flipkart.fdpinfra.kafka.balancer.utils.ZKUtil.ZK_SESSION_TIMEOUT;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import lombok.Data;
import lombok.extern.log4j.Log4j;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import scala.Option;
import scala.collection.Seq;

import com.flipkart.fdpinfra.kafka.balancer.model.Broker;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerMetaData;
import com.flipkart.fdpinfra.kafka.balancer.model.PartitionUpdateListener;

@Data
@Log4j
public class BrokersTopicsCache implements PartitionUpdateListener {

	private static final int FIRST_PARTITION = 0;
	private static BrokersTopicsCache BROKERS_TOPICS_CACHE_INSTANCE = new BrokersTopicsCache();

	private Map<String, Integer> topicReplicationFactorMap = new HashMap<>();
	private Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
	private Map<Integer, BrokerMetaData> brokerMetaDataMap = new HashMap<>();

	private static boolean INITIALIZED = false;

	private BrokersTopicsCache() {
		// Singleton
	}

	public static BrokersTopicsCache getInstance() {
		if (!INITIALIZED) {
			throw new RuntimeException("Instance not initialized. Use BrokersTopicsCache.initialize");
		}
		return BROKERS_TOPICS_CACHE_INSTANCE;
	}

	/**
	 * Initializes the BrokersTopicCache object by contacting the zookeeper and
	 * requesting relevant metadata.
	 *
	 * @param zookeeper
	 *            zookeeper observer url
	 * @param isRackEnabled
	 *            true if cluster is rack aware else false
	 */
	public static void initialize(String zookeeper, boolean isRackEnabled) {
		if (INITIALIZED) {
			throw new RuntimeException("Instance already initialized");
		}
		Set<String> topics = new HashSet<>();
		topics = BROKERS_TOPICS_CACHE_INSTANCE.getAllTopics(zookeeper);
		BROKERS_TOPICS_CACHE_INSTANCE.initializeTopicsMaps(zookeeper, topics);
		BROKERS_TOPICS_CACHE_INSTANCE.initializeBrokerMetaData(zookeeper, isRackEnabled);
		BROKERS_TOPICS_CACHE_INSTANCE.populateBrokerMetaData();
		INITIALIZED = true;
	}

	private void populateBrokerMetaData() {
		for (TopicMetadata topicMetadata : topicMetadataMap.values()) {
			for (PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
				int leader = partitionMetadata.leader().id();
				BrokerMetaData leaderBroker = brokerMetaDataMap.get(leader);
				if (leaderBroker != null) {
					leaderBroker.incrementNumLeaders();
				}
				for (Node replica : partitionMetadata.replicas()) {
					int follower = replica.id();
					if (leader != follower) {
						BrokerMetaData followerBroker = brokerMetaDataMap.get(follower);
						if (followerBroker != null) {
							followerBroker.incrementNumFollowers();
						}
					}
				}
			}
		}
		log.debug("Broker meta cache" + BROKERS_TOPICS_CACHE_INSTANCE.getBrokerMetaDataMap());
	}

	/**
	 * Generates a map of <broker_id, rack_id>
	 *
	 * @param zookeeper
	 *            zookeeper observer url
	 * @param isRackEnabled
	 *            true if cluster is rack aware else false
	 */
	private void initializeBrokerMetaData(String zookeeper, boolean isRackEnabled) {
		ZkUtils zkUtils = ZkUtils.apply(zookeeper, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, false);
		try {
			Seq<BrokerMetadata> brokersMetadataSeq = AdminUtils.getBrokerMetadatas(zkUtils,
					RackAwareMode.Enforced$.MODULE$, scala.Option.apply(null));
			List<BrokerMetadata> brokersMetadata = scalaSeqToJavaList(brokersMetadataSeq);
			for (BrokerMetadata brokerMetadata : brokersMetadata) {
				Option<String> rack = brokerMetadata.rack();
				String rackId;
				if (!isRackEnabled) {
					if (rack != null && !rack.isEmpty()) {
						throw new UnsupportedOperationException("Non RackAware mode selected but rack: " + rack.get().toString()
								+ " defined for broker: " + brokerMetadata.id() + ", cannot proceed, aborting. Run balancer with ---rack-aware flag");
					}
					rackId = null;
				} else if (rack == null || rack.isEmpty()) {
					throw new UnsupportedOperationException("RackAware mode selected but Rack not defined for broker: "
							+ brokerMetadata.id() + ", cannot proceed, aborting");
				} else {
					rackId = rack.get().toString();
				}
				brokerMetaDataMap.put(brokerMetadata.id(), new BrokerMetaData(rackId, 0, 0));
			}
		} finally {
			zkUtils.close();
		}
	}

	/**
	 * Populates topicReplicationFactor map and topicMetadataMap.
	 *
	 * @param zookeeperPath
	 *            zookeeper observer url
	 * @param topics
	 *            list of topics
	 */
	private void initializeTopicsMaps(String zookeeperPath, Set<String> topics) {
		String zkConnect = zookeeperPath;
		ZkUtils zkUtils = ZkUtils.apply(zkConnect, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, false);
		scala.collection.Set<TopicMetadata> topicsMetaDataSet = null;
		scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentMap = null;
		try {
			topicsMetaDataSet = AdminUtils.fetchTopicMetadataFromZk(
					scala.collection.JavaConversions.asScalaSet(topics), zkUtils);
			partitionAssignmentMap = zkUtils.getPartitionAssignmentForTopics(javaCollectionToScalaSeq(topics));
		} finally {
			zkUtils.close();
		}
		Set<TopicMetadata> topicsMetaData = scala.collection.JavaConversions.asJavaSet(topicsMetaDataSet);
		for (TopicMetadata topicMetadata : topicsMetaData) {
			topicMetadataMap.put(topicMetadata.topic(), topicMetadata);
		}

		Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignment = scalaMapToJavaMap(partitionAssignmentMap);
		for (Entry<String, scala.collection.Map<Object, Seq<Object>>> entry : partitionAssignment.entrySet()) {
			String topicName = entry.getKey();
			Map<Object, Seq<Object>> partitions = scalaMapToJavaMap(entry.getValue());
			Seq<Object> replicas = partitions.get(FIRST_PARTITION);
			if (replicas == null || replicas.isEmpty()) {
				throw new IllegalArgumentException("Replicas cannot be found for " + topicName + "-" + FIRST_PARTITION);
			}
			topicReplicationFactorMap.put(topicName, replicas.size());
		}
	}

	/**
	 * Fetch all topics from zookeeper.
	 *
	 * @param zookeeperPath
	 *            zookeeper observer url
	 * @return Set of all topics
	 */
	private Set<String> getAllTopics(String zookeeperPath) {
		ZkUtils zkUtils = ZkUtils.apply(zookeeperPath, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, false);
		Map<String, Properties> allTopicsPropertiesMap = new HashMap<>();
		try {
			allTopicsPropertiesMap = scala.collection.JavaConversions.asJavaMap(AdminUtils
					.fetchAllTopicConfigs(zkUtils));
		} finally {
			zkUtils.close();
		}
		log.info("Topics in the cluster: " + allTopicsPropertiesMap);
		return allTopicsPropertiesMap.keySet();
	}

	@Override
	public void update(Broker broker) {
		BrokerMetaData stats = brokerMetaDataMap.get(broker.getId());
		stats.setNumTotalFollowers(broker.getNumTotalFollowers());
		stats.setNumTotalLeaders(broker.getNumTotalLeaders());
	}
}