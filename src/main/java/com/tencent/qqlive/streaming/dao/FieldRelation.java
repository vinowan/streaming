package com.tencent.qqlive.streaming.dao;

import java.util.ArrayList;
import java.util.List;

import com.tencent.qqlive.streaming.util.IPInfo;
import com.tencent.qqlive.streaming.util.Utils;

public class FieldRelation {
	public static final int PROC_OPERATOR_GET_PROV = 1; //取得省份
	public static final int PROC_OPERATOR_GET_ISP = 2; //取得运营商
	public static final int PROC_OPERATOR_GET_HOST = 3; //取得CDN
	public static final int PROC_OPERATOR_GET_VID = 4; //取得节目
	public static final int PROC_OPERATOR_GET_DEVICE = 5; //取得设备
	public static final int PROC_OPERATOR_GET_NONE = 6; //使用原值
	public static final int PROC_OPERATOR_NOT_EQUAL = 7; //不等于
	public static final int PROC_OPERATOR_EQUAL = 8; //等于
	public static final int PROC_OPERATOR_LAGER_EQUAL = 9; //大于等于
	public static final int PROC_OPERATOR_SMALLER_EQUAL = 10; //小于等于
	public static final int PROC_OPERATOR_CONTAIN = 11; //包含
	
	private String item = null;
	private int procOperator = 0;
	private String value = null;
	
	public String getItem() {
		return item;
	}
	public void setItem(String item) {
		this.item = item;
	}
	public int getProcOperator() {
		return procOperator;
	}
	public void setProcOperator(int procOperator) {
		this.procOperator = procOperator;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	public boolean validate(int value) {
		return validate(String.valueOf(value));
	}
	
	public boolean validate(String value) {
		String result = null;
		
		switch (procOperator) {
		case PROC_OPERATOR_GET_PROV:
			result = IPInfo.getInstance().getIPBlock(value).province;
			break;
		case PROC_OPERATOR_GET_ISP:
			result = IPInfo.getInstance().getIPBlock(value).service;
			break;
		case PROC_OPERATOR_GET_HOST:
			result = Utils.getHostByUrl(value);
			break;
		case PROC_OPERATOR_GET_VID:
			result = Utils.getProgVid(value);
			break;
		default:
			result = value;
			break;
		}
		
		return validateItem(result);
	}
	
	@Override
	public String toString() {
		return String.format("%s:[%d,%s]", item, procOperator, value);
	}
	
	// relations example: err_code:[7,1001];err_code:[7,1003];err_code:[7,1004];err_code:[7,1005];err_code:[7,-1001];
	public static List<FieldRelation> valueOf(String relations) {
		List<FieldRelation> ret = new ArrayList<FieldRelation>();
		
		String[] items = relations.split(";");
		for (int i = 0; i<items.length; i++) {
			FieldRelation fr = new FieldRelation();
			String[] fields = items[i].split(":");
			if (fields.length != 2
					|| !fields[1].startsWith("[")
					|| !fields[1].endsWith("]"))
				continue;
			
			fr.setItem(fields[0].trim());
			String val = fields[1].substring(1, fields[1].length() - 1);
			
			String[] oper_value = val.split(",");
			if (oper_value.length != 2)
				continue;
			
			fr.setProcOperator(Integer.valueOf(oper_value[0]));
			fr.setValue(oper_value[1]);
			
			ret.add(fr);
		}
		
		return ret;
	}
	
	private boolean validateItem(String value) {
		Integer val1 = Utils.isNumeric(value);
		Integer val2 = Utils.isNumeric(this.value);
		
		if (val1 == null && val2 == null) {
			if (procOperator == PROC_OPERATOR_NOT_EQUAL) {
				return !value.contains(this.value);
			} else {
				return value.contains(this.value);
			}
		} else {
			if (val1 == null || val2 == null)
				return false;
			
			switch (procOperator) {
			case PROC_OPERATOR_NOT_EQUAL:
				return !val1.equals(val2);
			case PROC_OPERATOR_EQUAL:
				return val1.equals(val2);
			case PROC_OPERATOR_LAGER_EQUAL:
				return val1 >= val2;
			case PROC_OPERATOR_SMALLER_EQUAL:
				return val1 <= val2;
			default:
				return false;
			}
		}
	}
		
	public static void main(String[] args) throws Exception {
		List<FieldRelation> relations = FieldRelation.valueOf("err_code:[6,abc]");
		
		FieldRelation relation = (FieldRelation)relations.toArray()[0];
		System.out.println(relation.validate("ab"));
	}
}
