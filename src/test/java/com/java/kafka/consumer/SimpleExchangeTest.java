package com.java.kafka.consumer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.java.kafka.broker.SimpleBroker;
import com.java.kafka.data.City;
import com.java.kafka.producer.TestProducer;
import com.java.kafka.util.KafkaUtils;

public class SimpleExchangeTest {

	private SimpleBroker broker;
	private TestConsumer consumer;

	private Properties kafkaProperties;

	@Before
	public void setup() throws Exception {
		kafkaProperties = KafkaUtils.getProperties(KafkaUtils.BROKER_PROPERTIES);
		final Properties zookeeperProperties = KafkaUtils.getProperties(KafkaUtils.ZOOKEEPER_PROPERTIES);

		KafkaUtils.cleanKafkaHistory(kafkaProperties);

		broker = KafkaUtils.startKafkaBroker(kafkaProperties, zookeeperProperties);

		KafkaUtils.createTopic("simpletopic", 1,
				"localhost:" + Integer.valueOf(zookeeperProperties.getProperty("client.port")));
	}

	@Test
	public void testSimpleRun() throws Exception {
		final Properties consumerProperties = KafkaUtils.getProperties(KafkaUtils.CONSUMER_PROPERTIES);
		CountDownLatch receiveLatch = new CountDownLatch(1);
		consumer = new TestConsumer(consumerProperties, receiveLatch);
		consumer.run();

		Thread.sleep(4000);

		final Properties producerProperties = KafkaUtils.getProperties(KafkaUtils.PRODUCER_PROPERTIES);
		TestProducer producer = new TestProducer(producerProperties);
		Thread.sleep(4000);

		producer.delivery();
		receiveLatch.await();

		Map<City, Integer> summary = consumer.getSummary();
		try {
			assertEquals(10, summary.size());
			consumer.printSummary();
		} finally {
			consumer.close();
			System.out.println("consumer close");
			broker.stop();
			System.out.println("broker stopped");
		}
	}

	/**
	 * cleanup kafka logs and check if there are some remains
	 */
	@After
	public void cleanup() throws IOException {
		KafkaUtils.cleanKafkaHistory(kafkaProperties);
		System.out.println("After cleanup");
		Files.list(Paths.get(KafkaUtils.returnMainLogPath(kafkaProperties))).forEach(System.out::println);
	}

}
