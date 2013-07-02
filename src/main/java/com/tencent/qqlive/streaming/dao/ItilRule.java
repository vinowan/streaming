package com.tencent.qqlive.streaming.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ItilRule {
	public static final int COMPARE_TYPE_LAGRGER = 1; //>=
	public static final int COMPREA_TYPE_SMALLER = 2; //<=
	public static final int COMPARE_TYPE_NOT_EQUAL = 3; //!= 
	
	public static final int SUMMARY_TYPE_AVG = 1;  //平均
	public static final int SUMMARY_TYPE_SUM = 2; //求和
	public static final int SUMMARY_TYPE_COUNT = 3;//求个数
	
	private int itilID = 0;
	private ElementaryArithmetic arithExpr = null;
	private double motion = 0.0;
	private Map<String, ItemRange> itemRanges = null;
	private int compareType = 0; // 对应COMPARE_TYPE_LAGRGER
	private int zoomSize = 0;
	private String itilDesc = null;
	private String business = null;
	private String receiver = null;
	private List<FieldRelation> fieldRelations = null;
	private HourMotion hourMotion = null;
	private int msgWarnPeriod = 0;
	private int maxWarnCount = 0;
	
	public int getItilID() {
		return itilID;
	}
	public void setItilID(int itilID) {
		this.itilID = itilID;
	}
	public ElementaryArithmetic getArithExpr() {
		return arithExpr;
	}
	public void setArithExpr(ElementaryArithmetic arithExpr) {
		this.arithExpr = arithExpr;
	}
	public double getMotion() {
		return motion;
	}
	public void setMotion(double motion) {
		this.motion = motion;
	}
	public Map<String, ItemRange> getItemRanges() {
		return itemRanges;
	}
	public void setItemRanges(Map<String, ItemRange> itemRanges) {
		this.itemRanges = itemRanges;
	}
	public int getCompareType() {
		return compareType;
	}
	public void setCompareType(int compareType) {
		this.compareType = compareType;
	}
	public int getZoomSize() {
		return zoomSize;
	}
	public void setZoomSize(int zoomSize) {
		this.zoomSize = zoomSize;
	}
	public String getItilDesc() {
		return itilDesc;
	}
	public void setItilDesc(String itilDesc) {
		this.itilDesc = itilDesc;
	}
	public String getBusiness() {
		return business;
	}
	public void setBusiness(String business) {
		this.business = business;
	}
	public String getReceiver() {
		return receiver;
	}
	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}
	public List<FieldRelation> getFieldRelations() {
		return fieldRelations;
	}
	public void setFieldRelations(List<FieldRelation> fieldRelations) {
		this.fieldRelations = fieldRelations;
	}
	public HourMotion getHourMotion() {
		return hourMotion;
	}
	public void setHourMotion(HourMotion hourMotion) {
		this.hourMotion = hourMotion;
	}
	public int getMsgWarnPeriod() {
		return msgWarnPeriod;
	}
	public void setMsgWarnPeriod(int msgWarnPeriod) {
		this.msgWarnPeriod = msgWarnPeriod;
	}
	public int getMaxWarnCount() {
		return maxWarnCount;
	}
	public void setMaxWarnCount(int maxWarnCount) {
		this.maxWarnCount = maxWarnCount;
	}
	
	public Set<String> getExpression() {
		Set<String> result = new HashSet<String>();
		
		if (arithExpr != null) {
			result.addAll(arithExpr.getExpression());
		}
		
		if (fieldRelations != null) {
			for (FieldRelation relation : fieldRelations) {
				result.add(relation.getItem());
			}
		}
		
		return result;
	}
	
	public boolean validate(LogEntry entry) {
		for (FieldRelation relation : fieldRelations) {
			String value = entry.getFields().get(relation.getItem());
			if (value == null)
				return false;
			
			if (!relation.validate(value))
				return false;
		}
		return true;
	}
	
	public double calcContribRate(int hour, double splitResult, double totalResult, 
			int splitCount, int totalCount) {
		double contribRate = 0.0;
		
		List<String> notations = arithExpr.getPostfixNotation();
		if (notations.size() == 1 && notations.get(0).startsWith("[") 
				&& notations.get(0).endsWith("]")) {
			if (totalResult == 0)
				return -1;
			
			contribRate = splitResult * 100 / totalResult;
		} else {
			if (splitCount == 0 || totalCount == 0)
				return -1;
			
			HourMotion.Range range = hourMotion.getRange(hour);
			if (range == null)
				return -1;
			
			if (splitResult > range.getMax()) {
				contribRate = 100 * ((splitResult - range.getMax()) * splitCount)
						/ ((totalResult - range.getMax()) * totalCount);
			} else if (splitResult < range.getMin()) {
				contribRate = 100 * ((splitResult - range.getMin()) * splitCount)
						/ ((totalResult - range.getMin()) * totalCount);
			} else {
				contribRate = -1;
			}
		}
		
		return contribRate;
	}
	
	public double zoom(double value) {
		if (zoomSize == 0) {
			return value;
		} else if (zoomSize < 0) {
			return value / Math.abs(zoomSize);
		} else {
			return value * zoomSize;
		}
	}
}
