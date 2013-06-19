package com.tencent.qqlive.streaming.bolt;

import java.util.concurrent.atomic.AtomicLong;

public class BoltStatics {
	public AtomicLong inPacket = new AtomicLong();
	
	public String toStr() {
		StringBuilder sb = new StringBuilder();
		sb.append("inPacket: " + inPacket.get());
		return sb.toString();
	}
	
	public void reset() {
		inPacket.set(0);
	}
}
