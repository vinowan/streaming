package com.tencent.qqlive.streaming.spout;

import java.util.concurrent.atomic.AtomicLong;

public class SpoutStatics {
	public AtomicLong inPacket = new AtomicLong();
	public AtomicLong queueFull = new AtomicLong();
	public AtomicLong handlePacket = new AtomicLong();
	public AtomicLong wrongStreamPacket = new AtomicLong();
	public AtomicLong timeoutPacket = new AtomicLong();
	public AtomicLong invalidPlayidPacket = new AtomicLong();
	public AtomicLong emitPacket = new AtomicLong();
	
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
		sb.append("invalidPlayidPacket: " + invalidPlayidPacket.get());
		sb.append("\n");
		sb.append("emitPacket: " + emitPacket.get());
		sb.append("\n");
		
		return sb.toString();
	}
	
	public void reset() {
		inPacket.set(0);
		queueFull.set(0);
		handlePacket.set(0);
		wrongStreamPacket.set(0);
		timeoutPacket.set(0);
		invalidPlayidPacket.set(0);
		emitPacket.set(0);
	}
}
