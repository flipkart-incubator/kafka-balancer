package com.flipkart.fdpinfra.kafka.balancer.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j;

import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import com.flipkart.fdpinfra.kafka.balancer.model.Replica;

@Log4j
public class BalancerUtil {
	/**
	 * Converts leader and follower assignments to json and prints them on
	 * console.
	 *
	 * @param balancer
	 *            instance of KafkaTopicBalancer or UnderReplicatedTopicBalancer
	 * @return replica assignments as json string.
	 */
	public static String convertToJsonOutput(TopicMetadata topicMetadata, List<Replica> leaderAssignments,
			List<Replica> followerAssignments) {
		ArrayList<Replica> reassignList = new ArrayList<>(leaderAssignments);
		reassignList.addAll(followerAssignments);
		return convertToJsonOutput(reassignList, getCurrentPartitionState(topicMetadata), topicMetadata.topic());
	}

	/**
	 * Generates <partition, replicas> map.
	 *
	 * @param topicMetadata
	 *            TopicMetadata object for the current topic
	 * @return Map<partition,replicas>
	 */
	private static Map<Integer, List<Integer>> getCurrentPartitionState(TopicMetadata topicMetadata) {
		Map<Integer, List<Integer>> partitionData = new HashMap<>();
		List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionMetadata();
		for (PartitionMetadata partitionMetadata : partitionsMetadata) {
			List<Integer> replicas = new ArrayList<>();
			int leader = partitionMetadata.leader().id();
			replicas.add(leader);
			replicas.addAll(partitionMetadata.replicas().stream().map(node -> node.id()).filter(id -> id != leader)
					.collect(Collectors.toList()));
			partitionData.put(partitionMetadata.partition(), replicas);
		}
		return partitionData;
	}

	private static String convertToJsonOutput(List<Replica> reassignList, Map<Integer, List<Integer>> partitionData,
			String topic) {
		Map<Integer, List<Integer>> toolOutput = new HashMap<Integer, List<Integer>>();

		Map<Integer, List<Integer>> clonedPartitionData = clone(partitionData);

		int skipCount = 0;

		for (Replica replica : reassignList) {
			Integer brokerId = replica.getDstBrokerId();
			int partitionId = replica.getPartition();
			int oldBrokerId = replica.getSrcBrokerId();
			boolean isLeader = replica.isLeader();
			log.debug("Processing for partition:" + partitionId + " broker: " + brokerId + " leader:" + isLeader
					+ " oldid: " + oldBrokerId);

			List<Integer> list = partitionData.get(partitionId);

			// replace the replica map in the partitions.
			if (isLeader) {
				if (list.contains(brokerId)) {
					skipCount++;
					log.warn("Skipping the partition from re-assignment as Leader brokerid is already got added to the current partition: "
							+ partitionId + " brokerId " + brokerId + " list:" + list);
					continue;
				}
				list.remove(0);
				list.add(0, brokerId);
			} else {
				int index = oldBrokerId == -1 ? -1 : findIndex(list, oldBrokerId);
				if (list.contains(brokerId)) {
					skipCount++;
					log.warn("Skipping the partition from re-assignment as brokerid is already got added to the current partition: "
							+ partitionId + " brokerId " + brokerId + " list:" + list);
					continue;
				}
				if (index != -1) {
					list.remove(index);
					if (brokerId != -1) {
						list.add(index, brokerId);
					}
				} else {
					list.add(brokerId);
				}
			}

			toolOutput.put(partitionId, list);
		}

		if (skipCount > 0) {
			log.warn("Skipped due to collisions:" + skipCount);
		}
		List<String> diff = new ArrayList<>();
		int numProposedMovements = reassignList.size() - skipCount;
		if (numProposedMovements > 0) {
			getDiff(clonedPartitionData, toolOutput, diff);
			StringBuilder diffReadable = new StringBuilder();
			diff.forEach(d -> diffReadable.append(d).append("\n"));
			log.info("Assignment proposal for topic " + topic + " with number of total movements "
					+ numProposedMovements + "\n" + diffReadable.toString());
		}
		return convertToJsonOutput(topic, toolOutput);
	}

	private static String convertToJsonOutput(String topic, Map<Integer, List<Integer>> toolOutput) {

		int size = toolOutput.size();
		int count = 0;
		Set<Entry<Integer, List<Integer>>> entrySet = toolOutput.entrySet();

		StringBuilder jsonData = new StringBuilder();

		// TODO: use joiner
		for (Entry<Integer, List<Integer>> entry : entrySet) {
			jsonData.append(String.format(PARTITION_JSON, topic, entry.getKey(), entry.getValue()));
			count++;
			if (count != size) {
				jsonData.append(COMMA);
			}
		}
		return jsonData.toString();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map<Integer, List<Integer>> clone(Map<Integer, List<Integer>> partitionData) {
		Map<Integer, List<Integer>> clonedPartitionData = new HashMap<Integer, List<Integer>>();
		Set<Entry<Integer, List<Integer>>> entrySet2 = partitionData.entrySet();
		for (Entry<Integer, List<Integer>> entry : entrySet2) {
			clonedPartitionData.put(entry.getKey(), (ArrayList) ((ArrayList) entry.getValue()).clone());
		}
		return clonedPartitionData;
	}

	private static void getDiff(Map<Integer, List<Integer>> clonedPartitionData,
			Map<Integer, List<Integer>> toolOutput, List<String> diff) {
		Set<Entry<Integer, List<Integer>>> entrySet = toolOutput.entrySet();
		for (Entry<Integer, List<Integer>> entry : entrySet) {
			Integer partitionId = entry.getKey();
			List<Integer> proposed = entry.getValue();
			List<Integer> current = clonedPartitionData.get(partitionId);
			addDiff(partitionId, proposed, current, diff);
		}
	}

	private static void addDiff(Integer partitionId, List<Integer> proposed, List<Integer> current, List<String> diff) {
		StringBuilder builder = new StringBuilder("Partition: ");
		builder.append(partitionId).append(": (");
		if (proposed.size() >= current.size())
			diffForBalance(proposed, current, builder);
		else
			diffForRepDecrease(proposed, current, builder);
		builder.append(")");
		diff.add(builder.toString());
	}

	private static void diffForRepDecrease(List<Integer> proposed, List<Integer> current, StringBuilder builder) {
		int i = 0, j = 0;
		while (i < current.size() && j < proposed.size()) {
			if (i != 0) {
				builder.append(",");
			}
			if (current.get(i).equals(proposed.get(j))) {
				builder.append(current.get(i));
				i++;
				j++;
			} else {
				builder.append(current.get(i));
				builder.append("->del");
				i++;
			}
		}
		while (i < current.size()) {
			builder.append(",");
			builder.append(current.get(i));
			builder.append("->del");
			i++;
		}
	}

	private static void diffForBalance(List<Integer> proposed, List<Integer> current, StringBuilder builder) {
		int i = 0;
		for (i = 0; i < current.size(); i++) {
			if (i != 0) {
				builder.append(",");
			}
			builder.append(current.get(i));
			if (!current.get(i).equals(proposed.get(i))) {
				builder.append("->");
				builder.append(proposed.get(i));
			}
		}
		if (proposed.size() > current.size()) {
			for (; i < proposed.size(); i++) {
				builder.append(",urp->");
				builder.append(proposed.get(i));
			}
		}
	}

	private static int findIndex(List<Integer> list, Integer oldBrokerId) {

		for (int i = 0; i < list.size(); i++) {
			if (oldBrokerId.equals(list.get(i))) {
				return i;
			}
		}

		throw new IllegalArgumentException("Old broker " + oldBrokerId + " not found in the list " + list);
	}

	private static final String COMMA = ",";
	private static final String PARTITION_JSON = "{\"topic\": \"%s\",\"partition\": %s,\"replicas\": %s }";

}
