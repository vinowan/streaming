package com.tencent.qqlive.streaming.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static final Logger logger = LoggerFactory.getLogger(Config.class);
	
	// 相对zk.path的路径
	public static final String CONFIG_FILE_PATH = "/conf/stream";
	public static final String SPOUT_REGISTER_PATH = "/spout";
	
	private Properties prop = null;
	private String conf = null;
	
	public Config(String conf) {
		this.conf = conf;
	}
	
	public String get(String key) {
		if (prop == null) {
			prop = new Properties();
			try {
				prop.load(new StringReader(conf));
			} catch (IOException e) {
				logger.error("failed to load conf: " + conf + "\n" + StringUtils.stringifyException(e));
				return null;
			}
		}
		
		return prop.getProperty(key);
	}
}
