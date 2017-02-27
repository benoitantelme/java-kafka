package com.java.kafka.producer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.java.kafka.data.CitiesIndex;
import com.java.kafka.data.City;

public class Producer {
	protected KafkaProducer<String, String> producer;
	protected CitiesIndex index;
	protected Random randomGenerator = new Random();

	public Producer(Properties properties) throws Exception {
		producer = new KafkaProducer<>(properties);

		if (producer == null)
			throw new Exception("Could not initialise producer");

		index = new CitiesIndex("src/main/resources/worldcities.csv");
	}

	public void delivery() {
		producer.partitionsFor("topic");

		Thread producerThread = new Thread() {
			public void run() {
				for (int i = 0; i < 50; i++) {
					City city = index.getIndexElement(
							randomGenerator.ints(0, (index.getIndexSize())).findFirst().getAsInt());
					producer.send(new ProducerRecord<String, String>("topic", "city no " + i, city.toString()));
				}
			}
		};

		producerThread.start();
	}
	
	public void close(){
		producer.close();
	}

}
