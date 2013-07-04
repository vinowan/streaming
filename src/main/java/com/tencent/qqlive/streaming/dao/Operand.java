package com.tencent.qqlive.streaming.dao;

public class Operand {
	public static final int COMPUTE_TYPE_SUM = 1;
	public static final int COMPUTE_TYPE_CNT = 2;
	
	private String name = null;
	private int computeType = COMPUTE_TYPE_SUM;
	private double value = 0.0;
	private int count = 0;
	
	public Operand(String name) {
		this.name = name;
		
		if (name.startsWith("[") && name.endsWith("]"))
			computeType = COMPUTE_TYPE_CNT;
	}

	public String getName() {
		return name;
	}

	public int getComputeType() {
		return computeType;
	}

	public double getValue() {
		return value;
	}
	
	public int getCount() {
		return count;
	}

	public void compute(double val) {
		if (computeType == COMPUTE_TYPE_CNT) {
			value += 1;
		} else {
			value += val;
		}
		
		count += 1;
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Operand) {
			Operand oper = (Operand)obj;
			return name.equals(oper.name);
		}
		
		return false;
	}
	
	@Override
	public String toString() {
//		return name + ": (" + value + "," + count + ")";
		return String.format("%s:%d", name, value);
	}
}
