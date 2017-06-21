package com.flipkart.fdpinfra.kafka.balancer.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InputDataProvider {
	private FileDataProvider fileDataProvider = new FileDataProvider();

	public Object[][] getInputsFromTestConfig(String[] testSet) throws IOException {
		List<Object[]> inputs = new ArrayList<Object[]>();
		for (String testConfig : testSet) {
			TestSuite testSuite = getTestSuite(testConfig);
			inputs.addAll(getTestCases(testConfig, testSuite));
		}
		Object[][] result = convertListToArray(inputs);
		return result;
	}

	public TestSuite getTestSuite(String testConfig) throws IOException {
		TestSuite testSuite = TestUtil.convertToPojo(testConfig, TestSuite.class);
		fileDataProvider.addTestsFromFiles(testSuite);
		return testSuite;
	}

	/**
	 * Convert list of array to array of arrays
	 */
	public Object[][] convertListToArray(List<Object[]> inputs) {
		Object[][] result = new Object[inputs.size()][];
		for (int i = 0; i < result.length; i++) {
			result[i] = inputs.get(i);
		}
		return result;
	}

	/**
	 * Multiplex each of the test case with the inputConfig and return the
	 * result
	 */
	public List<Object[]> getTestCases(String testConfig, TestSuite testSuite) throws IOException {
		List<Object[]> testCases = new ArrayList<Object[]>();
		List<TestCase> tests = testSuite.getTestCases();
		for (TestCase testCase : tests) {
			addInputRow(testConfig, testCases, testCase);
		}
		return testCases;
	}

	public void addInputRow(String testConfig, List<Object[]> testCases, TestCase testCase) throws IOException {
		ArrayList<Object> inputRow = new ArrayList<Object>();
		inputRow.add(testCase.getTestCase());
		inputRow.add(testCase.getDescription());
		inputRow.add(testCase);
		testCases.add(inputRow.toArray());
	}
}
