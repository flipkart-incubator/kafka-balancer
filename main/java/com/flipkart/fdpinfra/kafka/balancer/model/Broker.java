package com.flipkart.fdpinfra.kafka.balancer.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class Broker extends BrokerMetaData implements Comparable<Broker> {

	private int id;

	private List<Integer> leaders = new ArrayList<Integer>();
	private List<Integer> followers = new ArrayList<Integer>();

	private PartitionUpdateListener partitionUpdateListener;

	Broker() {
		// dummy constructor for testing
	}

	public Broker(int id, int numTotalLeaders, int numTotalFollowers, PartitionUpdateListener listener) {
		this.id = id;
		this.numTotalLeaders = numTotalLeaders;
		this.numTotalFollowers = numTotalFollowers;
		this.partitionUpdateListener = listener;
	}

	public boolean hasPartition(int partition) {
		return leaders.contains(partition) || followers.contains(partition);
	}

	public List<Integer> getPartitions() {
		ArrayList<Integer> partitions = new ArrayList<>(leaders);
		partitions.addAll(followers);
		return Collections.unmodifiableList(partitions);
	}

	public List<Integer> getPartitions(boolean isLeader) {
		if (isLeader) {
			return Collections.unmodifiableList(leaders);
		} else {
			return Collections.unmodifiableList(followers);
		}
	}

	public void addPartition(Integer partition, boolean isLeader) {
		if (isLeader) {
			leaders.add(partition);
			incrementNumLeaders();
		} else {
			followers.add(partition);
			incrementNumFollowers();
		}

		if (partitionUpdateListener != null) {
			partitionUpdateListener.update(this);
		}
	}

	public void removePartition(int partition, boolean isLeader) {
		if (isLeader) {
			leaders.removeIf(p -> p == partition);
			decrementNumLeaders();
		} else {
			followers.removeIf(p -> p == partition);
			decrementNumFollowers();
		}

		if (partitionUpdateListener != null) {
			partitionUpdateListener.update(this);
		}
	}

	@Override
	public String toString() {
		return "Broker [ id=" + id + ", rack=" + rack + ", leaders=" + leaders + ", followers=" + getFollowers() + "]\n";
	}

	@Override
	public int compareTo(Broker that) {
		return Integer.compare(id, that.id);
	}

	public List<Integer> getLeaders() {
		return Collections.unmodifiableList(leaders);
	}

	public List<Integer> getFollowers() {
		return Collections.unmodifiableList(followers);
	}
	
	public int getNumFollowers(){
		return followers.size();
	}
}
