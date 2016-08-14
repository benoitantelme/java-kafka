package com.java.kafka.consumer;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.java.kafka.broker.SimpleBroker;
import com.java.kafka.producer.SimpleProducer;
import com.java.kafka.util.KafkaProcessCleaner;
import com.java.kafka.util.KafkaUtils;

public class SimpleConsumerTest {
	
	
	private static SimpleBroker broker;

	@BeforeClass
	public static void setup() throws Exception {
		final Properties kafkaProperties = KafkaUtils.getProperties(KafkaUtils.BROKER_PROPERTIES);
		final Properties zookeeperProperties = KafkaUtils.getProperties(KafkaUtils.ZOOKEEPER_PROPERTIES);
		// try to stop existing kafka/zookeeper process
		KafkaProcessCleaner.clean(kafkaProperties.getProperty("kafka.program.path"));

		KafkaUtils.cleanKafkaHistory(kafkaProperties);

		broker = KafkaUtils.startKafkaBroker(kafkaProperties, zookeeperProperties);
	}

	@Test
	public void testSimpleRun() throws Exception {
		final Properties consumerProperties = KafkaUtils.getProperties(KafkaUtils.CONSUMER_PROPERTIES);
		SimpleConsumer consumer = new SimpleConsumer(consumerProperties);
		consumer.run();

		final Properties producerProperties = KafkaUtils.getProperties(KafkaUtils.PRODUCER_PROPERTIES);
		SimpleProducer producer = new SimpleProducer(producerProperties);
		Thread.sleep(4000);

		producer.simpleDelivery();
		Thread.sleep(4000);

		Map<String, String> summary = consumer.getSummary();
		try {
			assertEquals(10, summary.size());
		} finally {
			broker.stop();
		}

	}

}
