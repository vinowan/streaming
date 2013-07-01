package com.tencent.qqlive.streaming.dao;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HourMotion {
	public static class Range {
		private double min = -1;
		private double max = -1;
		
		public double getMin() {
			return min;
		}

		public void setMin(double min) {
			this.min = min;
		}

		public double getMax() {
			return max;
		}

		public void setMax(double max) {
			this.max = max;
		}
		
		public boolean validate(double value) {
			//false，不需要告警
			boolean bRet1 = false, bRet2 = false;
			if(min != -1 && value < min)
			{
				bRet1 = true;
			}
			if(max != -1 && value > max)
			{
				bRet2 = true;
			}
			return bRet1||bRet2;
		}
		
		@Override
		public String toString() {
			return String.format("[%.2f~%.2f]", min, max);
		}
		
		public static Range valueOf(String value) {
			String[] min_max = value.split("~");
			if (min_max.length != 2)
				return null;
			
			Range range = new Range();
			range.setMin(Double.valueOf(min_max[0]));
			range.setMax(Double.valueOf(min_max[1]));
			
			return range;
		}
	}
	
	public static final int COMPARE_TYPE_NORMAL = 1;  //绝对值范围比较
	public static final int COMPARE_TYPE_MOM = 2;  //与上周同一时间段比较
	public static final int COMPARE_TYPE_INCR = 3; //以前一周期比较变化率
	
	private Map<Integer, Range> hourMotions = new HashMap<Integer, Range>();
	private String hourMotionsStr = null;

	private int compareType = COMPARE_TYPE_NORMAL;
	
	public String getHourMotionsStr() {
		return hourMotionsStr;
	}

	public void setHourMotionsStr(String hourMotionsStr) {
		this.hourMotionsStr = hourMotionsStr;
	}
	
	public int setCompareType(String compare) {
		if (compare.equalsIgnoreCase("INC")) {
			compareType = COMPARE_TYPE_INCR;
		} else if (compare.equalsIgnoreCase("MOM")) {
			compareType = COMPARE_TYPE_MOM;
		} else {
			compareType = COMPARE_TYPE_NORMAL;
		}
		
		return compareType;
	}
	
	// false无需告警，true需要告警
	public boolean validate(double result, int hour) {
		Range range = hourMotions.get(hour);
		if (range == null)
			return false; // 如果没有设置，则认为没有限制
		
		return range.validate(result);
	}
	
	public Range getRange(int hour) {
		return hourMotions.get(hour);
	}
	
	public void putRange(int hour, Range range) {
		hourMotions.put(hour, range);
	}
	
	public String getCompareTableName(long timestamp) {
		long ts = timestamp;
		
		switch (compareType) {
		case COMPARE_TYPE_INCR:
			ts -= 300*1000;
			break;
		case COMPARE_TYPE_MOM:
			ts -= 7*24*60*60*1000;
			break;
		case COMPARE_TYPE_NORMAL:
			return "";
		default:
			return "";
		}
		
		Date date = new Date(ts);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		String tableName = "t_monitor_" + formatter.format(date);
		
		return tableName;
	}
	
	public static HourMotion valueOf(String value) {
		HourMotion hourMotion = new HourMotion();
		
		String[] items = value.split(";");
		
		for (int i = 0; i<items.length; i++) {
			String item = items[i].trim();
			if (!item.startsWith("[") || !item.endsWith("]"))
				continue;
			
			item = item.substring(1, item.length() - 1);
			
			String[] hourRange_valueRange = item.split(":");
			Range range = Range.valueOf(hourRange_valueRange[1]);
			if (range == null)
				continue;
			
			String[] begin_end = hourRange_valueRange[0].split("~");
			if (begin_end.length == 1) {
				hourMotion.putRange(Integer.valueOf(begin_end[0]), range);
			} else {
				int begin = Integer.valueOf(begin_end[0]);
				int end = Integer.valueOf(begin_end[1]);
				
				for (int j = begin; j<=end; j++) {
					hourMotion.putRange(j, range);
				}
			}
		}
		
		hourMotion.setCompareType("NORMAL");
		hourMotion.setHourMotionsStr(value);
		
		return hourMotion;
	}
	
	public static void main(String[] args) {
		String value = "[0:50~100];[7~24:75~100];";
		
		HourMotion hourMotion = HourMotion.valueOf(value);
		
		for (Map.Entry<Integer, Range> entry : hourMotion.hourMotions.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue().toString());
		}
	}
}
