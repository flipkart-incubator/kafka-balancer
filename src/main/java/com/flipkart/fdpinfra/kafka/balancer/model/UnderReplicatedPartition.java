package com.flipkart.fdpinfra.kafka.balancer.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class UnderReplicatedPartition {
	private int partition, replicaDebt;

	@Override
	public String toString() {
		return "partition: " + partition + ", underRepCount: " + replicaDebt;
	}
}
