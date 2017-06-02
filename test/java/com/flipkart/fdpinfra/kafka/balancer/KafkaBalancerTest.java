package com.flipkart.fdpinfra.kafka.balancer;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.model.Broker;
import com.flipkart.fdpinfra.kafka.balancer.model.BrokerMetaData;
import com.flipkart.fdpinfra.kafka.balancer.model.UnderReplicatedPartition;
import com.flipkart.fdpinfra.kafka.balancer.test.InputDataProvider;
import com.flipkart.fdpinfra.kafka.balancer.test.TestCase;
import com.flipkart.fdpinfra.kafka.balancer.test.TestInput;

@Log4j
public class KafkaBalancerTest {

	private static final String BALANCE_PARTITIONS = "BALANCE_PARTITIONS";
	private static final String REASSIGN_UNDER_REPLICATED_PARTITIONS = "ReassignUnderReplicatedPartitions";
	private static final String REBALANCE_CLUSTER = "RebalanceCluster";
	private static final String SET_REPLICATION = "SetReplicationFactor";
	private static final String TOPIC_NAME = "testTopic";
	private static int TC_NO = 1;

	private String[] testSet = new String[] { "BalancerTestSuiteAddBrokers.yml", "UnderReplicatedPartitionsSuite.yml",
			"DecreaseReplicationFactorSuite.yml" };
	private InputDataProvider dataProvider = new InputDataProvider();

	@DataProvider
	public Object[][] getInputs() throws IOException {
		return dataProvider.getInputsFromTestConfig(testSet);
	}

	@Test(dataProvider = "getInputs")
	public void testBalancer(String testCaseName, String description, TestCase testCase) throws IOException,
			ReplicaAssignmentException {
		printCurrentTCDetails(testCaseName, testCase.getDescription());
		TestInput input = testCase.getInput();
		KafkaBalancer balancer = getBalancer(input);
		Map<Integer, BrokerMetaData> brokerMetaDataMap = setBalancerInputs(input, balancer);

		try {
			balancer.initializePartitionManager();
			balancer.generatePlan(input.getPartitionCount(), brokerMetaDataMap.size());
			validateOutput(testCase.getOutput(), balancer.getBrokersInfo().values());
			if (input.isErrorExpected()) {
				Assert.fail("Error is expected as per test input but no errors were reported by the program.");
			}
		} catch (Exception e) {
			if (input.isErrorExpected()) {
				validateErrorMessage(e.getMessage(), testCase.getOutput());
			} else {
				Assert.fail("Error encountered. " + e.getMessage());
			}
		}
	}

	private Map<Integer, BrokerMetaData> setBalancerInputs(TestInput input, KafkaBalancer balancer) {
		List<Broker> brokers = input.getBrokers();
		Map<Integer, Broker> brokersInfo = new HashMap<>();
		Map<Integer, BrokerMetaData> brokerMetaDataMap = new HashMap<>();
		TopicMetadata topicMetadata = new TopicMetadata(Errors.NONE, TOPIC_NAME, false, new ArrayList<>());
		for (int i = 0; i < input.getPartitionCount(); i++) {
			topicMetadata.partitionMetadata().add(
					new PartitionMetadata(Errors.NONE, i, Node.noNode(), new ArrayList<>(), null));
		}
		generateBrokerDetails(brokers, brokersInfo, topicMetadata, brokerMetaDataMap, balancer);
		balancer.setBrokersInfo(brokersInfo);
		balancer.setBrokerMetaDataMap(brokerMetaDataMap);
		balancer.setTopicMetadata(topicMetadata);
		List<UnderReplicatedPartition> underReplicatedPartitions = input.getUnderReplicatedPartitions();
		if (underReplicatedPartitions != null) {
			balancer.setUnderReplicatedPartitions(underReplicatedPartitions);
		}
		return brokerMetaDataMap;
	}

	/**
	 * Pass invalid brokerInfos and we should get an error
	 */
	@Test
	public void verifyPartitionPlacementRackValidator() {
		String desc = "Verify validatePartitionPlacements throws exception when rack contains multiple replicas for same partition.";
		printCurrentTCDetails("VerifyRackPlacementValidationLogic", desc);
		KafkaBalancer balancer = new TopicBalancer(TOPIC_NAME, true, BALANCE_PARTITIONS);

		Broker b1 = new Broker(1, 10, 20, null);
		Broker b2 = new Broker(2, 10, 20, null);
		Broker b3 = new Broker(3, 10, 20, null);
		Broker b4 = new Broker(4, 10, 20, null);

		b1.setRack("r1");
		b2.setRack("r2");
		b3.setRack("r3");
		b4.setRack("r1");

		b1.addPartition(0, true);
		b2.addPartition(1, true);
		b3.addPartition(2, true);
		b4.addPartition(3, true);

		b2.addPartition(0, false);
		b3.addPartition(0, false);

		b3.addPartition(1, false);
		b4.addPartition(1, false);

		b4.addPartition(2, false);
		b1.addPartition(2, false);

		b2.addPartition(3, false);
		b3.addPartition(3, false);

		Map<Integer, Broker> brokersInfoValid = new HashMap<>();

		Map<Integer, Broker> brokersInfoInvalid = new HashMap<>();
		brokersInfoInvalid.put(1, b1);
		brokersInfoInvalid.put(2, b2);
		brokersInfoInvalid.put(3, b3);
		brokersInfoInvalid.put(4, b4);

		try {
			balancer.partitionManager = new RackAwarePartitionManager(brokersInfoValid);
			balancer.partitionManager.validatePartitionPlacements(brokersInfoInvalid, 3);
			Assert.fail("Validate partition placement expected to fail but passed.");
		} catch (Exception e) {
			assertEquals(e.getMessage(),
					"Replica assignment failed since replica for partition:2 already exists on rack r1");
		}

	}

	/**
	 * Pass invalid brokerInfos and we should get an error
	 */
	@Test
	public void verifyPartitionPlacementBrokerValidator() {
		String description = "Verify validatePartitionPlacements throws exception when broker contains multiple replicas for same partition.";
		printCurrentTCDetails("VerifyBrokerPlacementValidationLogic", description);

		KafkaBalancer balancer = new TopicBalancer(TOPIC_NAME, true, BALANCE_PARTITIONS);

		Broker b1 = new Broker(1, 10, 20, null);
		Broker b2 = new Broker(2, 10, 20, null);
		Broker b3 = new Broker(3, 10, 20, null);

		b1.setRack("r1");
		b2.setRack("r2");
		b3.setRack("r3");

		b1.addPartition(0, true);
		b2.addPartition(1, true);
		b3.addPartition(2, true);

		b2.addPartition(0, false);
		b3.addPartition(0, false);

		b3.addPartition(1, false);
		b1.addPartition(1, false);

		b3.addPartition(2, false);
		b1.addPartition(2, false);

		Map<Integer, Broker> brokersInfoValid = new HashMap<>();

		Map<Integer, Broker> brokersInfoInvalid = new HashMap<>();
		brokersInfoInvalid.put(1, b1);
		brokersInfoInvalid.put(2, b2);
		brokersInfoInvalid.put(3, b3);

		try {
			balancer.partitionManager = new RackAwarePartitionManager(brokersInfoValid);
			balancer.partitionManager.validatePartitionPlacements(brokersInfoInvalid, 3);
			Assert.fail("Validate partition placement expected to fail but passed.");
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Replica assignment failed. Broker 3 already contains replica for partition 2");
		}
	}

	private void validateErrorMessage(String actual, String expected) {
		assertEquals(actual.toLowerCase(), jsonToErrorMessage(expected).toLowerCase());
	}

	private void printCurrentTCDetails(String testCaseName, String testCaseDescription) {
		log.info(System.getProperty("line.separator") + "**********************************************__TC_" + TC_NO++
				+ "__***************************************************");
		log.info("TC Name: " + testCaseName);
		log.info("TC Description: " + testCaseDescription);
	}

	private void validateOutput(String output, Collection<Broker> actualOutput) throws IOException {
		Collection<Broker> expectedOutput = jsonToBrokers(output);
		assertEquals(actualOutput, expectedOutput);
	}

	private void generateBrokerDetails(List<Broker> brokers, Map<Integer, Broker> brokerInfos,
			TopicMetadata topicMetadata, Map<Integer, BrokerMetaData> brokerMetaDataMap, KafkaBalancer balancer) {
		for (Broker broker : brokers) {
			brokerInfos.put(broker.getId(), broker);
			int numTotalLeaders = broker.getNumTotalLeaders();
			if (numTotalLeaders == 0) {
				numTotalLeaders = broker.getLeaders().size();
				broker.setNumTotalLeaders(numTotalLeaders);
			}
			int numTotalFollowers = broker.getNumTotalFollowers();
			if (numTotalFollowers == 0) {
				numTotalFollowers = broker.getFollowers().size();
				broker.setNumTotalFollowers(numTotalFollowers);
			}
			BrokerMetaData value = new BrokerMetaData(broker.getRack(), numTotalLeaders, numTotalFollowers);
			brokerMetaDataMap.put(broker.getId(), value);
			if (balancer instanceof ReplicationSetterBalancer)
				generateTopicMetadata(topicMetadata, broker);
		}
	}

	private void generateTopicMetadata(TopicMetadata topicMetadata, Broker broker) {
		List<PartitionMetadata> partitionMetadata = topicMetadata.partitionMetadata();
		for (Integer partition : broker.getFollowers()) {
			partitionMetadata.get(partition).replicas().add(new Node(broker.getId(), "host", 0, broker.getRack()));
		}
	}

	private List<Broker> jsonToBrokers(String jsonString) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		TypeFactory typeFactory = mapper.getTypeFactory();
		List<Broker> brokers = mapper.readValue(jsonString,
				typeFactory.constructCollectionType(List.class, Broker.class));
		return brokers;
	}

	private KafkaBalancer getBalancer(TestInput input) {
		KafkaBalancer balancer = null;
		String task = input.getTask();
		boolean rackAware = input.isRackAware();
		if (task.equalsIgnoreCase(REBALANCE_CLUSTER)) {
			balancer = new TopicBalancer(TOPIC_NAME, rackAware, BALANCE_PARTITIONS);
		} else if (task.equalsIgnoreCase(REASSIGN_UNDER_REPLICATED_PARTITIONS)) {
			balancer = new UnderReplicatedTopicBalancer(TOPIC_NAME, rackAware);
		} else if (task.equalsIgnoreCase(SET_REPLICATION)) {
			balancer = new ReplicationSetterBalancer(TOPIC_NAME, rackAware, input.getNewReplicationFactor());
		}
		balancer.setReplicationFactor(input.getReplicationFactor());
		return balancer;
	}

	private String jsonToErrorMessage(String jsonString) {
		JSONArray jsonArray = new JSONArray(jsonString);
		return jsonArray.getJSONObject(0).get("errorMessage").toString();
	}
}
