package com.flipkart.fdpinfra.kafka.balancer.model;

import java.util.Comparator;

public class AscendingDebtComparator extends DescendingDebtComparator {

	public AscendingDebtComparator(Comparator<BrokerDebt> loadComparator) {
		super(loadComparator);
	}

	@Override
	protected int compareDebt(BrokerDebt o1, BrokerDebt o2) {
		return o1.getDebt() - o2.getDebt();
	}
}