package com.flipkart.fdpinfra.kafka.balancer.test;

import java.util.List;

import com.flipkart.fdpinfra.kafka.balancer.model.Broker;
import com.flipkart.fdpinfra.kafka.balancer.model.UnderReplicatedPartition;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class TestInput {
	@JsonProperty
	private String task;

	@JsonProperty
	private int partitionCount;

	@JsonProperty
	private List<Broker> brokers;

	@JsonProperty
	private String description;

	@JsonProperty
	private List<UnderReplicatedPartition> underReplicatedPartitions;
	
	@JsonProperty
	private boolean rackAware;

	@JsonProperty
	private int replicationFactor = 3;
	
	@JsonProperty
	private int newReplicationFactor;

	@JsonProperty
	private boolean errorExpected;
}
