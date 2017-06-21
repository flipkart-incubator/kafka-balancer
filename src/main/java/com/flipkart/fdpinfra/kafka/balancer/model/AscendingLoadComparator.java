package com.flipkart.fdpinfra.kafka.balancer.model;

import java.util.Comparator;

public class AscendingLoadComparator implements Comparator<BrokerDebt> {

	@Override
	public int compare(BrokerDebt o1, BrokerDebt o2) {
		return o1.getNumTotalPartitions() - o2.getNumTotalPartitions();
	}

}
