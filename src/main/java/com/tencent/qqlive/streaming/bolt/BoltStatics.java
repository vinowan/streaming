package com.tencent.qqlive.streaming.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.tencent.qqlive.streaming.util.ComponentStats;

public class BoltStatics extends ComponentStats{
	public static class Statics {
		public AtomicLong inPacket = new AtomicLong();
		public AtomicLong wrongCategory = new AtomicLong();
		public AtomicLong wrongTuple = new AtomicLong();
		public AtomicLong wrongItil = new AtomicLong();
		public AtomicLong dimensionNull = new AtomicLong();
		
		public String toStr() {
			StringBuilder sb = new StringBuilder();
			sb.append("inPacket: " + inPacket.get());
			sb.append("\n");
			sb.append("wrongCategory: " + wrongCategory.get());
			sb.append("\n");
			sb.append("wrongTuple: " + wrongTuple.get());
			sb.append("\n");
			sb.append("wrongItil: " + wrongItil.get());
			sb.append("\n");
			sb.append("dimensionNull: " + dimensionNull.get());
			return sb.toString();
		}
		
		public void reset() {
			inPacket.set(0);
			wrongCategory.set(0);
			wrongTuple.set(0);
			wrongItil.set(0);
			dimensionNull.set(0);
		}
	}
	
	private String name = null;
	private Map<String, Statics> statics = new HashMap<String, Statics>();

	public BoltStatics(String name) {
		this.name = name;
	}
	
	public String toStr() {
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Statics> entry : statics.entrySet()) {
			sb.append(entry.getKey());
			sb.append("\n");
			sb.append(entry.getValue().toStr());
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public void reset() {
		for(Map.Entry<String, Statics> entry : statics.entrySet()) {
			entry.getValue().reset();
		}
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
	public String getComponentName() {
		return name;
	}
	
	public static void main(String[] args) {
		BoltStatics statics = new BoltStatics("bolt");
		statics.getStatics("abc").inPacket.getAndIncrement();
		System.out.println(statics.toStr());
	}
}
