package com.java.kafka.util;

import java.io.IOException;

import org.apache.log4j.Logger;

public class KafkaProcessCleaner {
	private static Logger logger = Logger.getLogger("KafkaProcessCleaner");

	public static void clean(String kafkBinPath) {
		String commandResult;
		try {
			commandResult = QueryExecutor
					.executeQuery(kafkBinPath + "kafka-server-stop.bat");

			logger.info("Tried to stop kafka, result: " + commandResult);
			
//			not working right now
//			commandResult = QueryExecutor
//					.executeQuery(kafkBinPath + "zookeeper-server-stop.bat");
//			logger.info("Tried to stop zookeeper, result: " + commandResult);
		} catch (IOException | InterruptedException e) {
			logger.error("Issue while executing initialization cleaning queries", e);
		}
	}

}
