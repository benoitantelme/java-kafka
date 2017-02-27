package com.java.kafka.broker;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.java.kafka.zookeeper.SimpleZookeeper;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class SimpleBroker {
	public KafkaServerStartable kafkaBroker;
	public SimpleZookeeper zookeeper;
	
	private static Logger logger = Logger.getLogger("SimpleBroker");
	
	public SimpleBroker(Properties kafkaProperties, Properties zkProperties) throws Exception{
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
		
		//create and start local zookeeper
		zookeeper = new SimpleZookeeper();
		zookeeper.startSimpleZookeeper(zkProperties);
		
		//create and start local kafka broker
		logger.info("Creating Kafka broker");
		kafkaBroker = new KafkaServerStartable(kafkaConfig);
		
		logger.info("Starting Kafka broker...");
		kafkaBroker.startup();
		logger.info("Kafka broker started");
	}
	
	
	public void stop() throws InterruptedException{
		logger.info("Stopping kafka...");
		if(kafkaBroker  != null)
			kafkaBroker.shutdown();
		logger.info("Done");
		
		logger.info("Stopping Zookeeper...");
		zookeeper.shutdown();
		logger.info("Done");
	}

}
