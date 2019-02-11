package com.mozcan.reactive_stream.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Messages {
	
	private static Properties properties;
	static {
		properties = new Properties();
		final InputStream stream = Messages.class.getResourceAsStream("/application.properties");
		if (stream == null) {
			throw new RuntimeException("No properties!!!");
		}
		try {
			properties.load(stream);
		} catch (final IOException e) {
			throw new RuntimeException("Configuration could not be loaded!");
		}finally {
			try {
				stream.close();
			} catch (IOException e) {
				throw new RuntimeException("Stream could not be closed!");
			}
		}
	}

	public static String getAsString(String key) {
		return properties.getProperty(key);
	}
}
