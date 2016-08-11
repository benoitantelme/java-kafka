package com.java.kafka.consumer;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import com.java.kafka.producer.SimpleProducer;

public class SimpleConsumerTest {
	
	@Test
	public void testSimpleRun() {
		SimpleConsumer consumer = new SimpleConsumer();
		consumer.run();
		
		SimpleProducer producer = new SimpleProducer();
		producer.simpleDelivery();
		
		Map<String, String> summary = consumer.getSummary();
		assertEquals(10, summary.size());
	}

}
