package com.tencent.qqlive.streaming.dao;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.tencent.qqlive.streaming.util.IPInfo;
import com.tencent.qqlive.streaming.util.Utils;

public class LatitudeRule {
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
	private double contribMinRate = 0.0;
	
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
	public double getContribMinRate() {
		return contribMinRate;
	}
	public void setContribMinRate(double contribMinRate) {
		this.contribMinRate = contribMinRate;
	}
	
	String getLatitudeValue(String value) {
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
	
	public static void main(String[] args) throws Exception {
		String url = "GT%2DS5360";
		System.out.println(URLDecoder.decode(url, "utf8"));
	}
}
