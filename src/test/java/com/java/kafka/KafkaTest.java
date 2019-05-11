package com.java.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class KafkaTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTest.class);
    private static TestUtil testUtil = TestUtil.getInstance();

    @BeforeClass
    public static void setup() {
        logger.info("----- Start setup -----");
        testUtil.prepare();
        logger.info("----- Setup done -----");
    }

    @Test
    public void kafkaSetupTest() throws InterruptedException {
        logger.info("----- Start test -----");

        CountDownLatch doneSignal = new CountDownLatch(2);

        MessageReceiver receiver = new MessageReceiver(doneSignal);
        receiver.setupReceiver();

        MessageProducer producer = new MessageProducer(doneSignal);
        producer.setupProducer();

        doneSignal.await();
        assertEquals(100, receiver.getMessagesCount());

        logger.info("----- Test done -----");
    }

    @AfterClass
    public static void tearDown() {
        logger.info("----- Start tear down -----");
        testUtil.tearDown();
        logger.info("----- Tear down done -----");
    }

}
