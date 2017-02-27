package com.java.kafka.consumer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import com.java.kafka.data.City;

public class TestConsumer extends Consumer{
	private CountDownLatch waitLatch;

	public TestConsumer(Properties properties, CountDownLatch waitLatch)
			throws Exception {
		super(properties, "simpletopic");
		this.waitLatch = waitLatch;
	}

	@Override
	protected void run() {
		Thread consumerThread = new Thread() {
			public void run() {
				try {
					while (!closed.get()) {
						ConsumerRecords<String, String> records = consumer.poll(100);
						if (records.count() != 0) {
							System.out.printf("Got %d records%n", records.count());

							for (ConsumerRecord<String, String> record : records) {
								history.put(City.fromString(record.value()), 1);
							}
							
							if(history.size() > 9)
								waitLatch.countDown();
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

}
