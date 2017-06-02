package com.flipkart.fdpinfra.kafka.balancer.utils;

import kafka.admin.ReassignPartitionsCommand;
import kafka.utils.ZkUtils;

public class ZKUtil {
	public static final int ZK_CONNECTION_TIMEOUT = 30000;
	public static final int ZK_SESSION_TIMEOUT = 30000;

	public static void executeAssignment(String zookeeper, String assignmentPlanJsonStr) {
		ZkUtils zkUtils = ZkUtils.apply(zookeeper, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, false);
		try {
			ReassignPartitionsCommand.executeAssignment(zkUtils, assignmentPlanJsonStr);
		} finally {
			zkUtils.close();
		}
	}
}
