package com.tencent.qqlive.streaming.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileRule {
	private Map<Integer, ItemRule> warningRules = null;
	private List<LatitudeRule> latitudeRules = null;
	private Set<String> exprs = null;
	
	public Map<Integer, ItemRule> getWarningRules() {
		return warningRules;
	}
	
	public void setWarningRules(Map<Integer, ItemRule> warningRules) {
		this.warningRules = warningRules;
	}
	
	public List<LatitudeRule> getLatitudeRules() {
		return latitudeRules;
	}
	
	public void setLatitudeRules(List<LatitudeRule> latitudeRules) {
		this.latitudeRules = latitudeRules;
	}
	
	public Set<String> getExprs() {
		if (exprs != null)
			return exprs;
		
		exprs = new HashSet<String>();
		for (Map.Entry<Integer, ItemRule> entry : warningRules.entrySet()) {
			exprs.addAll(entry.getValue().getExpression());
		}
		
		for (LatitudeRule lr : latitudeRules) {
			exprs.add(lr.getItemName());
		}
		
		return exprs;
	}	
}
