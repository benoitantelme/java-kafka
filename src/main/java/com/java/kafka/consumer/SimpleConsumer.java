package com.java.kafka.consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {
	private KafkaConsumer<String, String> consumer;
	private Map<String, String> history = new HashMap<>();

	public SimpleConsumer(Properties properties) throws Exception {
		consumer = new KafkaConsumer<>(properties);

		if (consumer == null)
			throw new Exception("Could not initialise consumer");

		consumer.subscribe(Arrays.asList("simpletopic"));
	}

	protected void run() {
		Thread consumerThread = new Thread() {
			public void run() {
				try {
					while (true) {
						ConsumerRecords<String, String> records = consumer.poll(100);
						if (records.count() != 0) {
							System.out.printf("Got %d records%n", records.count());

							int count = 0;
							for (ConsumerRecord<String, String> record : records) {
								history.put(record.key() + count++, record.toString());
							}
						}
					}
				} finally {
					consumer.close();
				}
			}
		};

		consumerThread.start();
	}

	protected Map<String, String> getSummary() {
		return history;
	}

}
