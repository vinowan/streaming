package com.tencent.qqlive.streaming.spout;

import java.util.concurrent.atomic.AtomicLong;

import com.tencent.qqlive.streaming.util.ComponentStats;

public class SpoutStatics extends ComponentStats {
	public AtomicLong inPacket = new AtomicLong();
	public AtomicLong queueFull = new AtomicLong();
	public AtomicLong handlePacket = new AtomicLong();
	public AtomicLong wrongStreamPacket = new AtomicLong();
	public AtomicLong timeoutPacket = new AtomicLong();
	public AtomicLong emitPacket = new AtomicLong();
	public AtomicLong noCategory = new AtomicLong();
	
	private String componentName = null;
	
	public SpoutStatics(String componentName) {
		this.componentName = componentName;
	}
	
	@Override
	public String toStr() {
		StringBuilder sb = new StringBuilder();
		sb.append("inPacket: " + inPacket.get());
		sb.append("\n");
		sb.append("queueFull: " + queueFull.get());
		sb.append("\n");
		sb.append("handlePacket: " + handlePacket.get());
		sb.append("\n");
		sb.append("wrongStreamPacket: " + wrongStreamPacket.get());
		sb.append("\n");
		sb.append("timeoutPacket: " + timeoutPacket.get());
		sb.append("\n");
		sb.append("emitPacket: " + emitPacket.get());
		sb.append("\n");
		sb.append("noCategory: " + noCategory.get());
		sb.append("\n");
		
		return sb.toString();
	}
	
	@Override
	public void reset() {
		inPacket.set(0);
		queueFull.set(0);
		handlePacket.set(0);
		wrongStreamPacket.set(0);
		timeoutPacket.set(0);
		emitPacket.set(0);
		noCategory.set(0);
	}

	@Override
	public String getComponentName() {
		return componentName;
	}
}
