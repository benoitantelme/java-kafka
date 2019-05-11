package com.java.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaLocalBroker {
    public List<KafkaServerStartable> kafka;
    private AdminClient adminClient;
    private List<NewTopic> topics;

    public KafkaLocalBroker(Properties kafkaProperties, int size) throws IOException, InterruptedException {
        KafkaConfig kafkaConfig;

        String logDir = kafkaProperties.getProperty("log.dirs");
        String listener = kafkaProperties.getProperty("listeners");
        String brokerId = kafkaProperties.getProperty("broker.id");
        String port = kafkaProperties.getProperty("port");

        // start local kafka brokers
        kafka = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            kafkaProperties.setProperty("broker.id", String.valueOf(Integer.valueOf(brokerId) + i));
            kafkaProperties.setProperty("log.dirs", logDir + i);
            kafkaProperties.setProperty("port", String.valueOf(Integer.valueOf(port) + i));
            kafkaProperties.setProperty("listeners", listener + String.valueOf(Integer.valueOf(port) + i));

            kafkaConfig = KafkaConfig.fromProps(kafkaProperties);
            kafka.add(new KafkaServerStartable(kafkaConfig));

        }

        adminClient = createAdminClient(port);
        topics = Arrays.asList(
                new NewTopic(MessageReceiver.OUTPUT_TOPIC, 1, (short) 1),
                new NewTopic(MessageReceiver.INPUT_TOPIC, 1, (short) 1)
        );
        adminClient.createTopics(topics);

    }

    private AdminClient createAdminClient(String port) throws IOException {
        Properties adminProperties = new Properties();
        adminProperties.load(Class.class.getResourceAsStream(
                "/admin-config.properties"));

        adminProperties.setProperty("bootstrap.servers", adminProperties.getProperty("bootstrap.servers") + port);
        adminProperties.setProperty("client.id", adminProperties.getProperty("client.id") + port);

        return AdminClient.create(adminProperties);
    }

    public void start() {
        for (KafkaServerStartable server : kafka)
            server.startup();
    }

    public void stop() {
        //        blocks for a while so can not rerun test directly with that
        //        adminClient.deleteTopics(topics.stream().map(topic -> topic.name()).collect(Collectors.toList()));

        for (KafkaServerStartable server : kafka)
            server.shutdown();
    }


}
