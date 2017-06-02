package com.flipkart.fdpinfra.kafka.balancer.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

/**
 * Provides input data from File/Directory
 */
public class FileDataProvider {

	private static final String CHARSET = "UTF-8";
	private static final String BACK_SLASH = "/";
	private static final String OUTPUT_FOLDER = "/output";
	private static final String INPUT_FOLDER = "/input";

	public void addTestsFromFiles(TestSuite testSuite) throws IOException {
		String baseDir = testSuite.getBaseDir();
		String resourcePath = getResourcePath(baseDir);
		String inputFoder = resourcePath + INPUT_FOLDER;
		String outputFolder = resourcePath + OUTPUT_FOLDER;
		List<File> inputFiles = new ArrayList<>();
		if (testSuite.getInclude() != null && !testSuite.getInclude().isEmpty()) {
			for (String file : testSuite.getInclude())
				inputFiles.add(new File(inputFoder + BACK_SLASH +file));
		}
		else
		{
			inputFiles = getInputFiles(inputFoder);
		}

		for (File inputFile : inputFiles) {
			File outputFile = new File(outputFolder + BACK_SLASH + inputFile.getName());
			addTestFromFile(testSuite, inputFile, outputFile);
		}
	}

	private String getResourcePath(String baseDir) {
		URL resource = Thread.currentThread().getContextClassLoader().getResource(baseDir);
		return resource.getPath();
	}

	public void addTestFromFile(TestSuite testSuite, File inputFile, File outputFile) throws IOException {
		String inputString = readFileAsString(inputFile);
		String outputString = readFileAsString(outputFile);
		addTestCase(testSuite, inputFile, inputString, outputString);
	}

	private String readFileAsString(File file) throws IOException {
		return IOUtils.toString(new FileInputStream(file), CHARSET);
	}

	private void addTestCase(TestSuite testSuite, File input, String inputString, String outputString)
			throws IOException {
		TestCase testCase = new TestCase();
		testCase.setInputAndExpectedOutput(inputString, outputString);
		testCase.setTestCase(input.getName());
		testCase.setDescription(testCase.getInput().getDescription());
		testSuite.getTestCases().add(testCase);
	}

	public List<File> getInputFiles(String inputFoder) {
		File folder = new File(inputFoder);
		File[] files = folder.listFiles();
		List<File> testInputs = new ArrayList<File>();
		for (File file : files) {
			testInputs.add(file);
		}
		return testInputs;
	}
}
