package com.java.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;

public class TestUtil {
    private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
    private static TestUtil instance = new TestUtil();

    private Random randPortGen = new Random(System.currentTimeMillis());
    private KafkaLocalBroker kafkaServer;
    private String hostname = "localhost";
    private int zkLocalPort;

    private TestUtil() {
        init();
    }

    public static TestUtil getInstance() {
        return instance;
    }

    private void init() {
        // get the localhost.
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("Error getting the value of localhost. " +
                    "Proceeding with 'localhost'.", e);
        }
    }

    private boolean startKafkaServer() {
        Properties kafkaProperties = new Properties();
        Properties zkProperties = new Properties();

        logger.info("Starting kafka server.");
        try {
            //load properties
            zkProperties.load(Class.class.getResourceAsStream(
                    "/zookeeper.properties"));

            while (true) {
                //start local Zookeeper
                try {
                    zkLocalPort = getNextPort();
                    // override the Zookeeper client port with the generated one.
                    zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort));
                    new ZookeeperLocal(zkProperties);
                    break;
                } catch (BindException bindEx) {
                    // bind exception. port is already in use. Try a different port.
                }
            }
            logger.info("ZooKeeper instance is successfully started on port " +
                    zkLocalPort);

            kafkaProperties.load(Class.class.getResourceAsStream(
                    "/kafka-server.properties"));
            // override the Zookeeper url.
            kafkaProperties.setProperty("zookeeper.connect", getZkUrl());
            while (true) {
//                kafkaLocalPort = getNextPort();
                // override the Kafka server port
//                kafkaProperties.setProperty("port", Integer.toString(kafkaLocalPort));
                kafkaServer = new KafkaLocalBroker(kafkaProperties, 3);
                try {
                    kafkaServer.start();
                    break;
                } catch (Exception bindEx) {
                    // let's try another port.
                }
            }
            logger.info("Kafka Server is successfully started");
            return true;

        } catch (Exception e) {
            logger.error("Error starting the Kafka Server.", e);
            return false;
        }
    }

//    public MessageAndMetadata getNextMessageFromConsumer(String topic) {
//        return getKafkaConsumer().getNextMessage(topic);
//    }

    public void prepare() {
        boolean startStatus = startKafkaServer();
        if (!startStatus) {
            throw new RuntimeException("Error starting the server!");
        }
        try {
            Thread.sleep(3 * 1000);   // add this sleep time to
            // ensure that the server is fully started before proceeding with tests.
        } catch (InterruptedException e) {
            // ignore
        }
//        getKafkaConsumer();
        logger.info("Completed the prepare phase.");
    }

    public void tearDown() {
        logger.info("Shutting down the kafka Server.");
        kafkaServer.stop();
        logger.info("Completed the tearDown phase.");
    }

    private synchronized int getNextPort() {
        // generate a random port number between 49152 and 65535
        return randPortGen.nextInt(65535 - 49152) + 49152;
    }

    public String getZkUrl() {
        return hostname + ":" + zkLocalPort;
    }

//    public String getKafkaServerUrl() {
//        return hostname + ":" + kafkaLocalPort;
//    }
}
