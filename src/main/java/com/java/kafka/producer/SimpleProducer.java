package com.java.kafka.producer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.java.kafka.data.CitiesIndex;
import com.java.kafka.data.City;

public class SimpleProducer {
	private KafkaProducer<String, String> producer;
	private CitiesIndex index;
	private Random randomGenerator= new Random();

	public SimpleProducer(Properties properties) throws Exception {
		producer = new KafkaProducer<>(properties);

		if (producer == null)
			throw new Exception("Could not initialise producer");
		
		index = new CitiesIndex("src/main/resources/worldcities.csv");
	}

	public void simpleDelivery() {
		producer.partitionsFor("simpletopic");
		for (int i = 0; i < 10; i++){
			City city = index.getIndexElement(randomGenerator.ints(0, (index.getIndexSize() + 1)).findFirst().getAsInt());
			producer.send(new ProducerRecord<String, String>("simpletopic", "city no " + i, city.toString()));
		}

		System.out.println("Message sent successfully");
		producer.close();
	}

}
