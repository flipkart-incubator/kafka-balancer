package com.flipkart.fdpinfra.kafka.balancer.model;

public interface PartitionUpdateListener {
	public void update(Broker broker);
}
