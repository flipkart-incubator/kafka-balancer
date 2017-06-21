package com.flipkart.fdpinfra.kafka.balancer.exception;

public class ReplicaAssignmentException extends Exception {
	private static final long serialVersionUID = 9104471675914274655L;

    public ReplicaAssignmentException(String message) {
        super(message);
    }

}
