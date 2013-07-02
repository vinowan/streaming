package com.tencent.qqlive.streaming.dao;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.tencent.qqlive.streaming.util.IPInfo;
import com.tencent.qqlive.streaming.util.Utils;

public class SegmentRule {
	public static class Segment {
		public static final int OPERATION_TYPE_GET_PROV = 1; //取得省份
		public static final int OPERATION_TYPE_GET_ISP = 2; //取得运营商
		public static final int OPERATION_TYPE_GET_HOST = 3; //取得CDN
		public static final int OPERATION_TYPE_GET_VID = 4; //取得节目
		public static final int OPERATION_TYPE_GET_DEVICE = 5; //取得设备
		public static final int OPERATION_TYPE_GET_NONE = 6; //使用原值
		public static final int OPERATION_TYPE_GET_URL = 7; //从携带vkey的URL中去掉vkey
		
		private String itemName = null;
		private int operation = OPERATION_TYPE_GET_NONE;
		private String category = null;
		
		public String getItemName() {
			return itemName;
		}
		public void setItemName(String itemName) {
			this.itemName = itemName;
		}
		public int getOperation() {
			return operation;
		}
		public void setOperation(int operation) {
			this.operation = operation;
		}
		public String getCategory() {
			return category;
		}
		public void setCategory(String category) {
			this.category = category;
		}
		
		String getValue(String value) {
			String sResult = "";
			switch(operation)
			{
			case OPERATION_TYPE_GET_PROV :
				sResult = IPInfo.getInstance().getIPBlock(value).province;
				break;
			case OPERATION_TYPE_GET_ISP :
				sResult = IPInfo.getInstance().getIPBlock(value).service;
				break;
			case OPERATION_TYPE_GET_HOST :
				sResult = Utils.getHostByUrl(value);
				break;
			case OPERATION_TYPE_GET_VID :
				sResult = Utils.getProgVid(value);
				break;
			case OPERATION_TYPE_GET_URL:
				sResult = Utils.getURLVKey(value);
				break;
			case OPERATION_TYPE_GET_DEVICE :
			case OPERATION_TYPE_GET_NONE :
				sResult = value;
				break;
			}
			
			try {
				sResult = URLDecoder.decode(sResult, "utf8");
			} catch (UnsupportedEncodingException e) {
				return sResult;
			}
			
			return sResult;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%d:%s", itemName, operation, category);
		}
		
		// example: remote:1:分地区;remote:2:运营商;
		public static List<Segment> valueof(String value) {
			List<Segment> segments = new ArrayList<Segment>();
			
			String[] segItems = value.split(";");
			for (String segVal : segItems) {
				String[] vals = segVal.split(":");
				if (vals.length != 3)
					continue;
				
				Segment seg = new Segment();
				seg.setItemName(vals[0]);
				seg.setOperation(Integer.valueOf(vals[1]));
				seg.setCategory(vals[2]);
				
				segments.add(seg);
			}
			
			return segments;
		}
	}
	
	public static class Category {
		private String category = null;
		private String value = null;
		
		public String getCategory() {
			return category;
		}
		public void setCategory(String category) {
			this.category = category;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		
		@Override
		public int hashCode() {
			int ret = 17;
			if (category != null)
				ret = 37*ret + category.hashCode();
			
			if (value != null)
				ret = 37*ret + value.hashCode();
			
			return ret;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Category) {
				Category cat = (Category)obj;
				return category.equals(cat.category) && value.equals(cat.value);
			}
			
			return false;
		}
	}
	
	private List<Segment> rules = null;
	private double contribMinRate = 0.0;
	
	public List<Segment> getRules() {
		return rules;
	}
	public void setRules(List<Segment> rules) {
		this.rules = rules;
	}
	public double getContribMinRate() {
		return contribMinRate;
	}
	public void setContribMinRate(double contribMinRate) {
		this.contribMinRate = contribMinRate;
	}
	
	public Category getCategory(Map<String, String> itemValues) {
		String[] catArray = new String[rules.size()];
		String[] valArray = new String[rules.size()];
		
		int i = 0;
		for (Segment seg : rules) {
			catArray[i] = seg.category;
			
			if (itemValues != null) {
				String val = itemValues.get(seg.getItemName());
				if (val == null)
					return null;
				
				valArray[i] = seg.getValue(val);
				if (valArray[i] == null)
					return null;
			}
			i++;
		}
		
		Category result = new Category();
		result.setCategory(Utils.join(catArray, ","));
		if (itemValues != null)
			result.setValue(Utils.join(valArray, ","));
		
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		List<Segment> segs = Segment.valueof("remote:1:分地区;remote:2:运营商;");
		SegmentRule rule = new SegmentRule();
		rule.setRules(segs);
		
		Category cat = rule.getCategory(null);
		System.out.println(cat.getCategory());
	}
}
