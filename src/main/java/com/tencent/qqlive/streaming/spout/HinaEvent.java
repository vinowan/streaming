package com.tencent.qqlive.streaming.spout;

import java.util.Map;

public class HinaEvent {
	private byte[] body = null;
	private Map<String, String> properties = null;
	
	public HinaEvent(byte[] body, Map<String, String> properties) {
		this.body = body;
		this.properties = properties;
	}
	
	public byte[] getBody() {
		return body;
	}
	
	public void setBody(byte[] body) {
		this.body = body;
	}
	
	public Map<String, String> getProperties() {
		return properties;
	}
	
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
}
