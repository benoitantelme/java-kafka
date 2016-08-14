package com.java.kafka.util;

import java.io.IOException;

import org.apache.log4j.Logger;

public class QueryExecutor {

	protected static String executeQuery(String query) throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder(query);

		final Process p = pb.start();

		StreamDumper errorDumper = new StreamDumper(p.getErrorStream());
		StreamDumper inputDumper = new StreamDumper(p.getInputStream());
		errorDumper.start();
		inputDumper.start();

		p.waitFor();

		String errorStr = errorDumper.outputStream.toString("UTF-8");
		if (!errorStr.trim().isEmpty()) {
			Logger.getLogger("QueryExecutor").info("[error]: {}" + errorStr);
		}

		String result = inputDumper.outputStream.toString("UTF-8");

		if (p.isAlive())
			p.destroy();

		return result;
	}

}
