package com.flipkart.fdpinfra.kafka.balancer;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeConst;
import static com.flipkart.fdpinfra.kafka.balancer.TopicBalancer.BALANCE_FOLLOWERS;
import static com.flipkart.fdpinfra.kafka.balancer.TopicBalancer.BALANCE_LEADERS;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import lombok.extern.log4j.Log4j;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import com.flipkart.fdpinfra.kafka.balancer.exception.ReplicaAssignmentException;
import com.flipkart.fdpinfra.kafka.balancer.utils.ZKUtil;

@Log4j
public class KafkaBalancerMain {

	private static final String COMMA = ",";

	public static void main(String[] args) throws Exception {
		ArgumentParser parser = argParser();
		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}

		String topicOrNull = ns.getString(TOPIC);
		String zookeeper = ns.getString(ZOOKEEPER);
		boolean rackAware = ns.getBoolean(RACK_AWARE);

		BrokersTopicsCache.initialize(zookeeper, rackAware);
		String assignmentPlanJsonStr = generateAssignmentPlan(ns, topicOrNull, rackAware);

		switch (ns.getString(OPERATION)) {
		case GENERATE:
			break;
		case EXECUTE:
			if (assignmentPlanJsonStr != null) {
				executePlan(zookeeper, assignmentPlanJsonStr);
			} else {
				log.info("No assignment plan generated to execute, exiting");
			}
			break;
		default:
			throw new ReplicaAssignmentException("Unknown mode: " + ns.getString(OPERATION));
		}
	}

	private static String generateAssignmentPlan(Namespace ns, String topicOrNull, boolean rackAware)
			throws ReplicaAssignmentException {
		List<String> generatedPlans = new ArrayList<>();

		if (topicOrNull == null || topicOrNull.isEmpty()) {
			Iterator<String> iterator = BrokersTopicsCache.getInstance().getTopicMetadataMap().keySet().iterator();
			while (iterator.hasNext()) {
				String topic = iterator.next();
				generateAssignmentPlan(ns, generatedPlans, topic, rackAware);
			}
		} else {
			generateAssignmentPlan(ns, generatedPlans, topicOrNull, rackAware);
		}

		if (generatedPlans.size() > 0) {
			StringBuilder assignmentPlan = new StringBuilder(JSON_HEADER);
			Iterator<String> iterator = generatedPlans.iterator();
			while (iterator.hasNext()) {
				assignmentPlan.append(iterator.next());
				if (iterator.hasNext()) {
					assignmentPlan.append(COMMA);
				}
			}
			assignmentPlan.append(JSON_FOOTER);
			String assignmentPlanJsonStr = assignmentPlan.toString();
			log.info("Assignment Plan:\n" + assignmentPlanJsonStr);
			return assignmentPlanJsonStr;
		}
		return null;
	}

	private static void generateAssignmentPlan(Namespace ns, List<String> assignmentPlans, String topic,
			boolean rackAware) throws ReplicaAssignmentException {
		KafkaBalancer balancer = null;
		Integer repFactor = ns.getInt(SET_REPLICATION_FACTOR);

		if (repFactor != -1) {
			System.out.println("Replication Factor Set: " + repFactor);
			balancer = new ReplicationSetterBalancer(topic, rackAware, repFactor);
		} else {
			String task = ns.getString(TASK);
			switch (task) {
			case REASSIGN_UNDER_REPLICATED_PARTITIONS:
				balancer = new UnderReplicatedTopicBalancer(topic, rackAware);
				break;
			case BALANCE_PARTITIONS:
			case BALANCE_FOLLOWERS:
			case BALANCE_LEADERS:
				balancer = new TopicBalancer(topic, rackAware, task);
				break;
			}
		}

		String plan = balancer.generateAssignmentPlan();
		if (plan != null && !plan.isEmpty()) {
			assignmentPlans.add(plan);
		}
	}

	private static void executePlan(String zookeeper, String assignmentPlanJsonStr) {
		log.info("Going to execute plan");
		Scanner reader = new Scanner(System.in);
		System.out.println("Assignment Plan: \n" + assignmentPlanJsonStr);
		System.out.println("Are you sure to execute the assignment? Enter yes/no to continue");
		String input = null;
		try {
			input = reader.next();
		} finally {
			reader.close();
		}
		if ("YES".equals(input.toUpperCase())) {
			log.info("Proceeding with execution ...");
			ZKUtil.executeAssignment(zookeeper, assignmentPlanJsonStr);
		} else {
			log.info("Not proceeding with execution as per user input: " + input);
		}
	}

	/**
	 * Creates and returns the ArgumentParser object for parsing command line
	 * arguments.
	 *
	 * @return the ArgumentParser object consisting of command line args.
	 */
	private static ArgumentParser argParser() {
		ArgumentParser parser = ArgumentParsers.newArgumentParser("KafkaBalancer").defaultHelp(true)
				.description("This tool is used to re-balance the kafka cluster.");

		parser.addArgument("--topic").action(store()).type(String.class).metavar("TOPIC").dest(TOPIC)
				.help("Topic on which the operation is to be performed.");

		parser.addArgument("--zookeeper").action(store()).required(true).type(String.class).dest(ZOOKEEPER)
				.help("zookeeper server URL.");

		MutuallyExclusiveGroup task = parser
				.addMutuallyExclusiveGroup()
				.required(true)
				.description(
						"--balance-partitions or --balance-leaders or --balance-followers or --reassign-under-replicated-partitions or --set-replication-factor must be specified");

		task.addArgument("--balance-partitions").setConst(BALANCE_PARTITIONS).action(storeConst()).dest(TASK)
				.required(false).type(String.class)
				.help("Rebalance cluster when there are no under-replicated partitions.");

		task.addArgument("--balance-leaders").setConst(BALANCE_LEADERS).action(storeConst()).dest(TASK).required(false)
				.type(String.class).help("Rebalance cluster but balance only leaders. Followers should be untouched");

		task.addArgument("--balance-followers").setConst(BALANCE_FOLLOWERS).action(storeConst()).dest(TASK)
				.required(false).type(String.class)
				.help("Rebalance cluster but balance only followers. Leaders should be untouched");

		task.addArgument("--reassign-under-replicated-partitions").setConst(REASSIGN_UNDER_REPLICATED_PARTITIONS)
				.action(storeConst()).dest(TASK).required(false).type(String.class)
				.help("Reassign under replicated partitions to online nodes.");

		task.addArgument("--set-replication-factor").action(store()).dest(SET_REPLICATION_FACTOR).required(false)
				.type(Integer.class).help("Set the number of replicas for a topic").setDefault(-1);

		MutuallyExclusiveGroup operation = parser.addMutuallyExclusiveGroup().required(true)
				.description("either --generate or --execute must be specified but not both.");

		operation.addArgument("--generate").setConst(GENERATE).action(storeConst()).dest(OPERATION).required(false)
				.type(String.class).help("Generate plan.");

		operation.addArgument("--execute").setConst(EXECUTE).action(storeConst()).dest(OPERATION).required(false)
				.type(String.class).help("Generate and execute plan.");

		parser.addArgument("--rack-aware").setConst(true).action(storeConst()).type(boolean.class).setDefault(false)
				.required(false).dest(RACK_AWARE).help("Consider racks while generating reassignment plan.");

		return parser;
	}

	private static final String JSON_HEADER = "{\"partitions\":[";
	private static final String JSON_FOOTER = "],\"version\":1}";
	private static final String OPERATION = "operation";
	private static final String TASK = "task";
	private static final String ZOOKEEPER = "zookeeper";
	private static final String TOPIC = "topic";
	private static final String BALANCE_PARTITIONS = "BalancePartitions";
	private static final String REASSIGN_UNDER_REPLICATED_PARTITIONS = "ReassignUnderReplicatedPartitions";
	private static final String SET_REPLICATION_FACTOR = "SetReplicationFactor";
	private static final String GENERATE = "generate";
	private static final String EXECUTE = "execute";
	private static final String RACK_AWARE = "rack_aware";
}
