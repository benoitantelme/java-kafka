package com.java.kafka.consumer;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.BeforeClass;
import org.junit.Test;

import com.java.kafka.broker.SimpleBroker;
import com.java.kafka.data.City;
import com.java.kafka.producer.SimpleProducer;
import com.java.kafka.util.KafkaUtils;

public class SimpleConsumerTest {
	
	
	private static SimpleBroker broker;

	@BeforeClass
	public static void setup() throws Exception {
		final Properties kafkaProperties = KafkaUtils.getProperties(KafkaUtils.BROKER_PROPERTIES);
		final Properties zookeeperProperties = KafkaUtils.getProperties(KafkaUtils.ZOOKEEPER_PROPERTIES);

		KafkaUtils.cleanKafkaHistory(kafkaProperties);

		broker = KafkaUtils.startKafkaBroker(kafkaProperties, zookeeperProperties);
		
		int zookeeperPort = Integer.valueOf(zookeeperProperties.getProperty("client.port"));

		KafkaUtils.createTopic("simpletopic", 1, "localhost:" + zookeeperPort);
	}

	@Test
	public void testSimpleRun() throws Exception {
		final Properties consumerProperties = KafkaUtils.getProperties(KafkaUtils.CONSUMER_PROPERTIES);
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch receiveLatch = new CountDownLatch(1);
		SimpleConsumer consumer = new SimpleConsumer(consumerProperties, startLatch, receiveLatch);
		consumer.run();

		startLatch.await();
		
		Thread.sleep(4000);
		
		final Properties producerProperties = KafkaUtils.getProperties(KafkaUtils.PRODUCER_PROPERTIES);
		SimpleProducer producer = new SimpleProducer(producerProperties);
		Thread.sleep(4000);

		producer.simpleDelivery();
		
		receiveLatch.await();

		Map<String, City> summary = consumer.getSummary();
		try {
			assertEquals(10, summary.size());
			consumer.printSummary();
		} finally {
			broker.stop();
		}

	}

}
