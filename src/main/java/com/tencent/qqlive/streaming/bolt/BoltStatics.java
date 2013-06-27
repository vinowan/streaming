package com.tencent.qqlive.streaming.bolt;

import java.util.concurrent.atomic.AtomicLong;

import com.tencent.qqlive.streaming.util.ComponentStats;

public class BoltStatics extends ComponentStats{
	public AtomicLong inPacket = new AtomicLong();
	
	private String name = null;
	
	public BoltStatics(String name) {
		this.name = name;
	}
	
	public String toStr() {
		StringBuilder sb = new StringBuilder();
		sb.append("inPacket: " + inPacket.get());
		return sb.toString();
	}
	
	public void reset() {
		inPacket.set(0);
	}

	@Override
	public String getComponentName() {
		return name;
	}
}
