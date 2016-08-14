package com.java.kafka.zookeeper;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public class SimpleZookeeper {
	private ZooKeeperServerMain zooKeeperServer;
	private static Logger logger = Logger.getLogger("SimpleZookeeper");

	public void startSimpleZookeeper(Properties properties) throws Exception {
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(properties);
		} catch (Exception e) {
			logger.error("Issue while parsing zookeeper configuration", e);
			throw new RuntimeException(e);
		}

		zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);

		new Thread() {
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					logger.error("Issue while running zookeeper", e);
				}
			}
		}.start();
	}

}
