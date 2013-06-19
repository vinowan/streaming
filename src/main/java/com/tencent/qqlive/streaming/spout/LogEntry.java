package com.tencent.qqlive.streaming.spout;

public class LogEntry {
	private long timestamp = 0;
	private String album = null;
	private String playid = null;
	private String devtype = null;
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getAlbum() {
		return album;
	}
	public void setAlbum(String album) {
		this.album = album;
	}
	public String getPlayid() {
		return playid;
	}
	public void setPlayid(String playid) {
		this.playid = playid;
	}
	public String getDevtype() {
		return devtype;
	}
	public void setDevtype(String devtype) {
		this.devtype = devtype;
	}
	
	@Override
	public int hashCode() {
		int ret = 17;
		ret = ret * 31 + playid.hashCode();
		ret = ret * 31 + devtype.hashCode();
		return ret;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof LogEntry) {
			LogEntry other = (LogEntry)obj;
			return playid.equals(other.playid) && devtype.equals(other.devtype);
		}
		
		return false;
	}
}
