package com.flipkart.fdpinfra.kafka.balancer.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class TestUtil {
	private static final JsonFactory jsonFactory = new YAMLFactory();

	public static <T> T convertToPojo(String configFilePath, Class<T> clazz) throws IOException {
		URL resource = Thread.currentThread().getContextClassLoader().getResource(configFilePath);
		return buildPojoFromYaml(resource.getPath(), clazz);
	}

	public static <T> T buildPojoFromYaml(String configName, Class<T> clazz) throws IOException {
		ObjectMapper mapper = new ObjectMapper(jsonFactory);
		InputStream inputStream = new FileInputStream(configName);
		return mapper.readValue(inputStream, clazz);
	}

	public static <T> T readJson(String jsonString, Class<T> clazz) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(jsonString, clazz);
	}
}
