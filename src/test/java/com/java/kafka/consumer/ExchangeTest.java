package com.java.kafka.consumer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.java.kafka.broker.SimpleBroker;
import com.java.kafka.producer.Producer;
import com.java.kafka.util.KafkaUtils;

public class ExchangeTest {

	private SimpleBroker broker;
	private Consumer consumer;

	private Properties kafkaProperties;

	@Before
	public void setup() throws Exception {
		kafkaProperties = KafkaUtils.getProperties(KafkaUtils.BROKER_PROPERTIES);
		final Properties zookeeperProperties = KafkaUtils.getProperties(KafkaUtils.ZOOKEEPER_PROPERTIES);

		KafkaUtils.cleanKafkaHistory(kafkaProperties);

		broker = KafkaUtils.startKafkaBroker(kafkaProperties, zookeeperProperties);

		KafkaUtils.createTopic("topic", 1,
				"localhost:" + Integer.valueOf(zookeeperProperties.getProperty("client.port")));
	}

//	@Test
	public void testRun() throws Exception {
		final Properties consumerProperties = KafkaUtils.getProperties(KafkaUtils.CONSUMER_PROPERTIES);
		consumer = new Consumer(consumerProperties, "topic");
		consumer.run();

		Thread.sleep(4000);

		final Properties producerProperties = KafkaUtils.getProperties(KafkaUtils.PRODUCER_PROPERTIES);
		Producer producer = new Producer(producerProperties);
		Thread.sleep(4000);

		while (consumer.getSummary().size() < 500) {
			producer.delivery();
		}
		
		consumer.printSummary();

		consumer.close();
		System.out.println("consumer close");
		broker.stop();
		System.out.println("broker stopped");
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
