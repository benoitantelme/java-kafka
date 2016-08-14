package com.java.kafka.broker;

import java.util.Properties;

import com.java.kafka.zookeeper.SimpleZookeeper;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class SimpleBroker {
	
	public KafkaServerStartable kafka;
	public SimpleZookeeper zookeeper;
	
	public SimpleBroker(Properties kafkaProperties, Properties zkProperties) throws Exception{
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
		
		//start local zookeeper
		System.out.println("starting local zookeeper...");
		zookeeper = new SimpleZookeeper();
		zookeeper.startSimpleZookeeper(zkProperties);
		System.out.println("done");
		
		//start local kafka broker
		kafka = new KafkaServerStartable(kafkaConfig);
		System.out.println("starting local kafka broker...");
		kafka.startup();
		System.out.println("done");
	}
	
	
	public void stop(){
		//stop kafka broker
		System.out.println("stopping kafka...");
		kafka.shutdown();
		System.out.println("done");
	}

}
