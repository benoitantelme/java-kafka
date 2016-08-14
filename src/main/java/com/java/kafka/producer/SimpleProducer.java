package com.java.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {
	private KafkaProducer<String, String> producer;

	public SimpleProducer(Properties properties) throws Exception {
		producer = new KafkaProducer<>(properties);

		if (producer == null)
			throw new Exception("Could not initialise producer");
	}

	public void simpleDelivery() {
		producer.partitionsFor("simpletopic");
		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<String, String>("simpletopic", "not much key" + i, "not much here"));

		System.out.println("Message sent successfully");
		producer.close();
	}

}
