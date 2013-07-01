package com.tencent.qqlive.streaming.dao;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileRule {
	private Map<Integer, ItemRule> warningRules = null;
	private Map<String, SegmentRule> segmentRules = null;
	private Set<String> exprs = null;
	
	public Map<Integer, ItemRule> getWarningRules() {
		return warningRules;
	}
	
	public void setWarningRules(Map<Integer, ItemRule> warningRules) {
		this.warningRules = warningRules;
	}
	
	public Map<String, SegmentRule> getSegmentRules() {
		return segmentRules;
	}
	
	public void setSegmentRules(Map<String, SegmentRule> segmentRules) {
		this.segmentRules = segmentRules;
	}
	
	public Set<String> getExprs() {
		if (exprs != null)
			return exprs;
		
		exprs = new HashSet<String>();
		for (Map.Entry<Integer, ItemRule> entry : warningRules.entrySet()) {
			exprs.addAll(entry.getValue().getExpression());
		}
		
		for (Map.Entry<String, SegmentRule> entry : segmentRules.entrySet()) {
			for (SegmentRule.Segment seg : entry.getValue().getRules()) {
				exprs.add(seg.getItemName());
			}
		}
		
		return exprs;
	}	
}
