package com.flipkart.fdpinfra.kafka.balancer.test;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonProperty;

@Getter
@ToString
public class TestSuite {

	@JsonProperty
	private String testSuiteName;

	@JsonProperty
	private List<TestCase> testCases = new ArrayList<TestCase>();

	@JsonProperty
	private String baseDir;
	
	@JsonProperty
	private String description;
	
	@JsonProperty
	private List<String> include;
}
