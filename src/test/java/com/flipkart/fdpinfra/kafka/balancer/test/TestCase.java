package com.flipkart.fdpinfra.kafka.balancer.test;

import java.io.IOException;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class TestCase {

	@JsonProperty
	private String testCase;

	@JsonProperty
	private String description;

	@JsonProperty
	private TestInput input;

	@JsonProperty
	private String output;

	public void setInputAndExpectedOutput(String inputString, String outputString) throws IOException {
		this.setInput(TestUtil.readJson(inputString, TestInput.class));
		this.setOutput(outputString);
	}
}
