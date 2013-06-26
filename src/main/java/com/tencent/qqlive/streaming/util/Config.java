package com.tencent.qqlive.streaming.util;

import java.util.Map;

public class Config {
	public static String getString(Map conf, String key, String default_value) {
		Object o = conf.get(key);
		if (o != null) {
			return o.toString();
		} else {
			return default_value;
		}
	}

	public static int getInt(Map conf, String key, int default_value) {
		Object o = conf.get(key);
		if (o != null) {
			return Integer.parseInt(o.toString());
		} else {
			return default_value;
		}
	}

	public static boolean getBoolean(Map conf, String key, boolean default_value) {
		Object o = conf.get(key);
		if (o != null) {
			if (o.toString().equalsIgnoreCase("true"))
				return true;
			else
				return false;
		} else {
			return default_value;
		}
	}

	public static long getLong(Map conf, String key, long default_value) {
		Object o = conf.get(key);
		if (o != null) {
			return Long.parseLong(o.toString());
		} else {
			return default_value;
		}
	}
}
