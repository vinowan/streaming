package com.tencent.qqlive.streaming.dao;

import java.util.HashMap;
import java.util.Map;

public class ItemRange {
	private String item = null;
	private double min = -1;
	private double max = -1;
	
	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

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
		boolean ret = true;
		boolean ret1 = true, ret2 = true;
		if(max != -1)
		{
			ret1 = (value <= max);
		}

		if(min != -1)
		{
			ret2 = (value >= min);
		}
		ret = (ret1 && ret2);
		return ret;
	}
	
	@Override
	public String toString() {
		return String.format("%s:[%.2f,%.2f]", item, min, max);
	}
	// range example: errcode:[1,-1];ptick:[1,180000];
	public static Map<String, ItemRange> valueOf(String ranges) {
		Map<String, ItemRange> ret = new HashMap<String, ItemRange>();
		
		String[] items = ranges.split(";");
		for (int i = 0; i<items.length; i++) {
			ItemRange ir = new ItemRange();
			String[] fields = items[i].split(":");
			if (fields.length != 2
					|| !fields[1].startsWith("[")
					|| !fields[1].endsWith("]"))
				continue;
			
			ir.setItem(fields[0].trim());
			String val = fields[1].substring(1, fields[1].length() - 1);
			
			String[] min_max = val.split(",");
			if (min_max.length != 2)
				continue;
			
			ir.setMin(Double.valueOf(min_max[0]));
			ir.setMax(Double.valueOf(min_max[1]));
			
			ret.put(ir.getItem(), ir);
		}
		
		return ret;
	}
	
	public static void main(String[] args) {
		String ranges = "errcode:[1,-1];ptick:[1,180000]";
		
//		List<ItemRange> result = ItemRange.valueOf(ranges);
//		
//		for(ItemRange range : result) {
//			System.out.println(range.toString());
//		}
	}
}
