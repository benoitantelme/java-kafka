package com.java.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.java.kafka.data.City;

public class TestProducer extends Producer{

	public TestProducer(Properties properties) throws Exception {
		super(properties);
	}

	@Override
	public void delivery() {
		producer.partitionsFor("simpletopic");
		for (int i = 0; i < 10; i++){
			City city = index.getIndexElement(randomGenerator.ints(0, (index.getIndexSize())).findFirst().getAsInt());
			producer.send(new ProducerRecord<String, String>("simpletopic", "city no " + i, city.toString()));
		}

		System.out.println("Message sent successfully");
		close();
	}

}
