package com.flipkart.fdpinfra.kafka.balancer.model;

import java.util.Comparator;

public class DescendingLoadComparator implements Comparator<BrokerDebt> {

	@Override
	public int compare(BrokerDebt o1, BrokerDebt o2) {
		return o2.getNumTotalPartitions() - o1.getNumTotalPartitions();
	}

}