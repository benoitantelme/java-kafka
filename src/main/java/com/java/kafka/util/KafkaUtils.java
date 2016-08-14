package com.java.kafka.util;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.java.kafka.broker.SimpleBroker;

public class KafkaUtils {
	public static final String BROKER_PROPERTIES = "broker.properties";
	public static final String ZOOKEEPER_PROPERTIES = "zookeeper.properties";
	public static final String PRODUCER_PROPERTIES = "producer.properties";
	public static final String CONSUMER_PROPERTIES = "consumer.properties";
	
	private static Logger logger = Logger.getLogger("KafkaUtils");

	public static Properties getProperties(String propertiesPath) throws Exception {
		final Properties properties = new Properties();
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();

		try (final InputStream stream = classloader.getResourceAsStream(propertiesPath)) {
			properties.load(stream);
		}

		if (!properties.isEmpty())
			return properties;
		else
			throw new Exception("Properties from " + propertiesPath + " are empty.");
	}

	public static void cleanKafkaHistory(Properties kafkaProperties) {
		try {
			String logs = kafkaProperties.getProperty("main.log.dirs");
			FileUtils.cleanDirectory(new File(logs));
		} catch (Exception e) {
			logger.error("Issue while cleaning kafka history", e);
		}
	}

	public static SimpleBroker startKafkaBroker(Properties kafkaProperties, Properties zookeeperProperties) {
		SimpleBroker broker = null;
		try {
			broker = new SimpleBroker(kafkaProperties, zookeeperProperties);
		} catch (Exception e) {
			Logger.getLogger("SimpleConsumerTest").error("Issue while starting the kafka broker", e);
			throw new RuntimeException(e);
		}

		return broker;
	}
}
