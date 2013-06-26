package com.tencent.qqlive.streaming.spout;

public class HinaEvent {
	private byte[] body = null;
	private String category = null;

	public HinaEvent(byte[] body, String category) {
		this.body = body;
		this.category = category;
	}
	
	public byte[] getBody() {
		return body;
	}
	
	public void setBody(byte[] body) {
		this.body = body;
	}
	
	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}
}
