package com.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class MessageProducer {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private CountDownLatch doneSignal;

    public MessageProducer(CountDownLatch doneSignal) {
        this.doneSignal = doneSignal;

    }

    public void setupProducer() {
        logger.info("MessageProducer setup");
        KafkaProducer<String, String> producer = createKafkaProducer("prod-0");

        new Thread(() -> {
            try {
                int counter = 5;
                while (counter > 0) {
                    logger.info("MessageProducer start sending messages");

                    for (int i = 0; i < 20; i++) {
                        final ProducerRecord<String, String> record =
                                new ProducerRecord<>(MessageReceiver.INPUT_TOPIC, String.valueOf(i) + String.valueOf(counter),
                                        "Hello " + counter + i);
                        RecordMetadata metadata = producer.send(record).get();
                    }

                    logger.info("MessageProducer stop sending messages and sleep");

                    counter--;
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                producer.flush();
                producer.close();
                doneSignal.countDown();
            }
        }).start();
    }

    protected static KafkaProducer<String, String> createKafkaProducer(String id) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
//        props.put(TRANSACTIONAL_ID_CONFIG, id);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(props);

    }
}
