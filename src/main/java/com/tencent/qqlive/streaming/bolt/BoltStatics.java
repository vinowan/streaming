package com.tencent.qqlive.streaming.bolt;

import java.util.concurrent.atomic.AtomicLong;

import com.tencent.qqlive.streaming.util.ComponentStats;

public class BoltStatics extends ComponentStats{
	public AtomicLong inPacket = new AtomicLong();
	public AtomicLong wrongCategory = new AtomicLong();
	public AtomicLong wrongTuple = new AtomicLong();
	public AtomicLong wrongItil = new AtomicLong();
	public AtomicLong dimensionNull = new AtomicLong();
	
	private String name = null;
	
	public BoltStatics(String name) {
		this.name = name;
	}
	
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

	@Override
	public String getComponentName() {
		return name;
	}
}
