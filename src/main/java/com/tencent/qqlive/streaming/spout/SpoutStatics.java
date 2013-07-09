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
//	public AtomicLong emitStream = new AtomicLong();
	
	public static class Statics {
		public AtomicLong emitStream = new AtomicLong();
		public AtomicLong noValidStream = new AtomicLong();
		
		public String toStr() {
			StringBuilder sb = new StringBuilder();
			sb.append("emitStream: " + emitStream.get());
			sb.append("\n");
			sb.append("noValidStream: " + noValidStream.get());
			
			return sb.toString();
		}
		
		public void reset() {
			emitStream.set(0);
			noValidStream.set(0);
		}
	}
	
	private Map<String, Statics> statics = new HashMap<String, Statics>();
	
	private String componentName = null;
	
	public SpoutStatics(String componentName) {
		this.componentName = componentName;
	}
	
	public Statics getStatics(String category) {
		Statics stat = statics.get(category);
		if (stat == null) {
			stat = new Statics();
			statics.put(category, stat);
		}
		
		return stat;
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
		
		for (Map.Entry<String, Statics> entry : statics.entrySet()) {
			sb.append(entry.getKey());
			sb.append("\n");
			sb.append(entry.getValue().toStr());
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
//		emitStream.set(0);
		
		for(Map.Entry<String, Statics> entry : statics.entrySet()) {
			entry.getValue().reset();
		}
	}

	@Override
	public String getComponentName() {
		return componentName;
	}
}
