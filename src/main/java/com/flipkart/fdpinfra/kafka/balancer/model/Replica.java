package com.flipkart.fdpinfra.kafka.balancer.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Replica {

	private int partition;
	private boolean isLeader;
	private int srcBrokerId;
	private int dstBrokerId;

	@Override
	public String toString() {
		return "Replica [partition=" + partition + ", isLeader=" + isLeader + ", srcBrokerId=" + srcBrokerId
				+ ", dstBrokerId=" + dstBrokerId + "] \n";
	}
}
