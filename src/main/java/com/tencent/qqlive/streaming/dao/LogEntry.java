package com.tencent.qqlive.streaming.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tencent.qqlive.streaming.util.Utils;

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
	
	public String encodeFields() {
		List<String> body = new ArrayList<String>();
		for (Map.Entry<String, String> field : fields.entrySet()) {
			body.add(new String(field.getKey() + "=" + field.getValue()));
		}
		
		return Utils.join(body, "\t");
	}
	
	public void decodeFields(String body) {
		String[] items = body.split("\\s+");
		for (String item : items) {
			String[] key_value = item.split("\\s*=\\s*");
			fields.put(key_value[0], key_value[1]);
		}
	}
	
	public static void main(String[] args) {
		String line = "remote=123.148.139.193 qq=0 devid=a49bb198d861e6b8 devtype=2";
		LogEntry log = new LogEntry();
		log.decodeFields(line);
		
		System.out.println(log.encodeFields());
	}
}
