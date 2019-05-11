package com.java.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class MessageReceiver {
    public static final String CONSUMER_GROUP_ID = "my-group-id";
    public static final String OUTPUT_TOPIC = "output";
    public static final String INPUT_TOPIC = "input";

    private static final Logger logger = LoggerFactory.getLogger(MessageReceiver.class);

    private Map<String, Integer> wordCountMap = new HashMap<>();
    private CountDownLatch doneSignal;
    public MessageReceiver(CountDownLatch doneSignal){
        this.doneSignal = doneSignal;

    }

    public void setupReceiver() {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        new Thread(() -> {
            try {
                int counter = 0;
                while (wordCountMap.size() < 100) {
                    logger.info("MessageReceiver consumer poll");
                    ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));
                    records.records(new TopicPartition(INPUT_TOPIC, 0))
                            .stream()
                            .forEach(record -> wordCountMap.merge(record.value(), 1, (old, one) -> old+one));

                    Thread.sleep(1000);

                    if(counter > 25)
                        break;
                    else
                        counter ++;
                }
                doneSignal.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }


    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singleton(INPUT_TOPIC));
        return consumer;
    }

    protected int getMessagesCount(){
        return wordCountMap.size();
    }

}
