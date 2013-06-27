package com.tencent.qqlive.streaming.spout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.tencent.qqlive.streaming.util.ComponentStats;

public class SpoutStatics extends ComponentStats {
	public AtomicLong inPacket = new AtomicLong();
	public AtomicLong queueFull = new AtomicLong();
	public AtomicLong handlePacket = new AtomicLong();
	public AtomicLong wrongStreamPacket = new AtomicLong();
	public AtomicLong timeoutPacket = new AtomicLong();
	public AtomicLong noCategory = new AtomicLong();
	
	private Map<String, AtomicLong> emitStream = new HashMap<String, AtomicLong>();
	
	private String componentName = null;
	
	public SpoutStatics(String componentName) {
		this.componentName = componentName;
	}
	
	public void incr(String stream) {
		AtomicLong stats = emitStream.get(stream);
		if (stats == null) {
			stats = new AtomicLong(0);
			emitStream.put(stream, stats);
		}
		
		stats.incrementAndGet();
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
		sb.append("noCategory: " + noCategory.get());
		sb.append("\n");
		
		for (Map.Entry<String, AtomicLong> entry : emitStream.entrySet()) {
			sb.append(entry.getKey() + ": " + entry.getValue().get());
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	@Override
	public void reset() {
		inPacket.set(0);
		queueFull.set(0);
		handlePacket.set(0);
		wrongStreamPacket.set(0);
		timeoutPacket.set(0);
		noCategory.set(0);
		
		for (Map.Entry<String, AtomicLong> entry : emitStream.entrySet()) {
			entry.getValue().set(0);
		}
	}

	@Override
	public String getComponentName() {
		return componentName;
	}
}
