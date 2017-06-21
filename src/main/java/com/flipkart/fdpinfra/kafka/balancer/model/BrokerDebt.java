package com.flipkart.fdpinfra.kafka.balancer.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class BrokerDebt {

	private int debt;
	private Broker broker;
	private boolean isLeaderDebt;
	
	public BrokerDebt(int debt, Broker broker, boolean isLeaderDebt) {
		this.debt = debt;
		this.broker = broker;
		this.isLeaderDebt = isLeaderDebt;
	}

	@Override
	public String toString() {
		return "BrokerDebt [broker=" + broker + " debt=" + debt + "]\n";
	}

	public int getNumTotalPartitions(){
		if(isLeaderDebt){
			return broker.getNumTotalLeaders();
		}
		return broker.getNumTotalFollowers();
	}
}
