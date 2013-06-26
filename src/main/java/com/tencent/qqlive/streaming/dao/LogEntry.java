package com.tencent.qqlive.streaming.dao;

import java.util.HashMap;
import java.util.Map;

public class LogEntry {
	private String category = null;
	private long timestamp = 0;
	private Map<String, String> fields = new HashMap<String, String>();
	
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, String> getFields() {
		return fields;
	}
	public void addField(String key, String value) {
		fields.put(key, value);
	}
}
