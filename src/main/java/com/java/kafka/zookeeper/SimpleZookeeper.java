package com.java.kafka.zookeeper;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class SimpleZookeeper {
	private ServerCnxnFactory factory;
	private ZooKeeperServer zooKeeperServer;
	private static Logger logger = Logger.getLogger("SimpleZookeeper");

	public void startSimpleZookeeper(Properties properties) throws Exception {
		File snapDirectory = new File(properties.getProperty("snap.dir"));
		File logDirectory = new File(properties.getProperty("log.dir"));
		int tickTime = Integer.valueOf(properties.getProperty("tick.time"));

		logger.info("Creating zookeeper");
		zooKeeperServer = new ZooKeeperServer(snapDirectory, logDirectory, tickTime);

		int clientPort = Integer.valueOf(properties.getProperty("client.port"));

		factory = NIOServerCnxnFactory.createFactory();
		factory.configure(new InetSocketAddress("localhost", clientPort), 16);
		factory.startup(zooKeeperServer);
		logger.info("Zookeeper started");
	}

	public void shutdown() throws InterruptedException {
			zooKeeperServer.shutdown();
			factory.shutdown();
	}

}
