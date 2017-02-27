package com.java.kafka.consumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.java.kafka.data.City;

public class Consumer {
	protected KafkaConsumer<String, String> consumer;
	protected Map<City, Integer> history = new ConcurrentHashMap<>();
	protected final AtomicBoolean closed = new AtomicBoolean(false);

	public Consumer(Properties properties, String topicName) throws Exception {
		consumer = new KafkaConsumer<>(properties);

		if (consumer == null)
			throw new Exception("Could not initialise consumer");

		consumer.subscribe(Arrays.asList(topicName));
	}

	public void close() {
		closed.set(true);
		consumer.wakeup();
	}

	protected void run() {
		Thread consumerThread = new Thread() {
			public void run() {
				try {
					while (!closed.get()) {
						ConsumerRecords<String, String> records = consumer.poll(100);
						if (records.count() != 0) {
							System.out.printf("Got %d records%n", records.count());

							for (ConsumerRecord<String, String> record : records) {
								City city = City.fromString(record.value());
								updateHistory(city);
							}
						}
					}
				} catch (WakeupException e) {
					// Ignore exception if closing
					if (!closed.get()) {
						throw e;
					}
				} finally {
					consumer.close();
				}
			}
		};

		consumerThread.start();
	}

	protected Map<City, Integer> getSummary() {
		return history;
	}

	protected void updateHistory(City city) {
		Integer times = history.get(city);
		
		if (times == null)
			times = new Integer(0);

		history.put(city, times + 1);
	}

	protected void printSummary() {
		for (Entry<City, Integer> entry : history.entrySet()) {
			System.out.println("City " + entry.getKey().toString() + " has appeared " + entry.getValue() + " times");
		}
	}

}
