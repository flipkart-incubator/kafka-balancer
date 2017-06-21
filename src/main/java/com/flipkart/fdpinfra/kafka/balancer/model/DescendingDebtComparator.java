package com.flipkart.fdpinfra.kafka.balancer.model;

import java.util.Comparator;

public class DescendingDebtComparator implements Comparator<BrokerDebt> {
	
	private Comparator<BrokerDebt> loadComparator;

	public DescendingDebtComparator(Comparator<BrokerDebt> loadComparator){
		this.loadComparator = loadComparator;
	}
	
	protected int compareDebt(BrokerDebt o1, BrokerDebt o2) {
		return o2.getDebt() - o1.getDebt();
	}
	
	protected int compareNumTotalPartitions(BrokerDebt o1, BrokerDebt o2) {
		return o1.getNumTotalPartitions() - o2.getNumTotalPartitions();
	}

	@Override
	public int compare(BrokerDebt o1, BrokerDebt o2) {
		int debtCompare = compareDebt(o1, o2);
		if (debtCompare != 0) {
			return debtCompare;
		}

		int loadCompare = loadComparator.compare(o1, o2);
		if(loadCompare != 0){
			return loadCompare;
		}

		return o1.getBroker().compareTo(o2.getBroker());
	}
}