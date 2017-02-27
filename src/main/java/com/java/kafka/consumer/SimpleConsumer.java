package com.java.kafka.consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.java.kafka.data.City;

public class SimpleConsumer {
	private KafkaConsumer<String, String> consumer;
	private Map<String, City> history = new HashMap<>();
	private CountDownLatch startLatch;
	private CountDownLatch awaitLatch;

	public SimpleConsumer(Properties properties, CountDownLatch startLatch, CountDownLatch awaitLatch)
			throws Exception {
		this.startLatch = startLatch;
		this.awaitLatch = awaitLatch;
		consumer = new KafkaConsumer<>(properties);

		if (consumer == null)
			throw new Exception("Could not initialise consumer");

		consumer.subscribe(Arrays.asList("simpletopic"));
	}

	protected void run() {
		Thread consumerThread = new Thread() {
			public void run() {
				try {
					startLatch.countDown();
					while (true) {
						ConsumerRecords<String, String> records = consumer.poll(100);
						if (records.count() != 0) {
							System.out.printf("Got %d records%n", records.count());

							int count = 0;
							for (ConsumerRecord<String, String> record : records) {
								history.put(record.key(), City.fromString(record.value()));
							}
							awaitLatch.countDown();
						}
					}
				} finally {
					consumer.close();
				}
			}
		};

		consumerThread.start();
	}

	protected Map<String, City> getSummary() {
		return history;
	}

	protected void printSummary() {
		for (Entry<String, City> entry : history.entrySet()) {
			System.out.println("Entry with key " + entry.getKey() + " and with city " + entry.getValue().toString());
		}
	}

}
