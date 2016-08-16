package com.java.kafka.util;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.log4j.Logger;

import com.java.kafka.broker.SimpleBroker;

import kafka.admin.TopicCommand;
import kafka.utils.ZkUtils;

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
	
	 public static void createTopic(String topicName, Integer numPartitions, String zookeeperAddress) {
	        String[] arguments = new String[9];
	        arguments[0] = "--create";
	        arguments[1] = "--zookeeper";
	        arguments[2] = zookeeperAddress;
	        arguments[3] = "--replication-factor";
	        arguments[4] = "1";
	        arguments[5] = "--partitions";
	        arguments[6] = "" + Integer.valueOf(numPartitions);
	        arguments[7] = "--topic";
	        arguments[8] = topicName;
	        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

	        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
	                30000, 30000, JaasUtils.isZkSecurityEnabled());

	        logger.info("CreateTopic " + Arrays.toString(arguments));
	        TopicCommand.createTopic(zkUtils, opts);
	    }

}
