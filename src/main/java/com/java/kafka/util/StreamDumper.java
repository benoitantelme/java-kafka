package com.java.kafka.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

/**
 * Used to run windows commands from java and stop kafka/zookeeper if it is
 * running
 *
 */
public class StreamDumper extends Thread {
	InputStream inputStream;
	ByteArrayOutputStream outputStream;

	StreamDumper(InputStream inputStream) {
		this.inputStream = inputStream;
		this.outputStream = new ByteArrayOutputStream();
	}

	public void run() {
		try {
			byte[] buffer = new byte[1024];
			int length;
			while ((length = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, length);
			}
		} catch (IOException ioe) {
			Logger.getLogger("StreamDumper").error("Issue while buffering during the call.", ioe);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				Logger.getLogger("StreamDumper").error("Issue while closing the input stream", e);
			}
			try {
				if (outputStream != null)
					outputStream.close();
			} catch (IOException e) {
				Logger.getLogger("StreamDumper").error("Issue while closing the output stream", e);
			}
		}
	}

}
