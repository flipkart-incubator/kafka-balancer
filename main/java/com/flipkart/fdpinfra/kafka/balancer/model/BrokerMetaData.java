package com.flipkart.fdpinfra.kafka.balancer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode(exclude = { "numTotalLeaders", "numTotalFollowers" })
public class BrokerMetaData {
	protected String rack;
	protected int numTotalLeaders;
	protected int numTotalFollowers;

	BrokerMetaData() {
		// Empty default constructor for testing
	}

	public void incrementNumLeaders() {
		numTotalLeaders += 1;
	}

	public void incrementNumFollowers() {
		numTotalFollowers += 1;
	}

	public void decrementNumLeaders() {
		numTotalLeaders -= 1;
		if (numTotalLeaders < 0) {
			throw new IllegalArgumentException("numTotalLeaders cannot be less than zero : " + numTotalLeaders);
		}
	}

	public void decrementNumFollowers() {
		numTotalFollowers -= 1;
		if (numTotalLeaders < 0) {
			throw new IllegalArgumentException("numTotalLeaders cannot be less than zero : " + numTotalFollowers);
		}
	}
}
