package com.tencent.qqlive.streaming.dao;

public class Expression {
	public static final int COMPUTE_TYPE_SUM = 1;
	public static final int COMPUTE_TYPE_CNT = 2;
	
	private String name = null;
	private long value = 0;
	private int itemCnt = 0;
	
	private int computeType = COMPUTE_TYPE_SUM;
	
	public Expression(String name) {
		if (name.startsWith("[") && name.endsWith("]")) {
			computeType = COMPUTE_TYPE_CNT;
			this.name = name.substring(1, name.length() - 1);
		} else {
			this.name = name;
		}
	}
	
	public String getName() {
		return name;
	}
	
	public long getValue() {
		return value;
	}
	
	public int getItemCnt() {
		return itemCnt;
	}
	
	public void addValue(long val) {
		switch (computeType) {
		case COMPUTE_TYPE_SUM:
			value += val;
			itemCnt++;
			break;
		case COMPUTE_TYPE_CNT:
			value++;
			itemCnt++;
			break;
		default:
			break;
		}
	}
	
	public void reset() {
		value = 0;
		itemCnt = 0;
	}
}
