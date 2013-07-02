package com.tencent.qqlive.streaming;

import java.sql.Connection;

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.WarningDataDao;


public class Main {
	public static void main(String[] args) throws Exception {
		DatabaseConnection dbConn = new DatabaseConnection("10.156.36.29", "3306", "root", "");
//		DatabaseConnection dbConn = new DatabaseConnection("10.130.2.16", "3306", "p2p", "!@#$qwerASDFzxcv");
		Connection conn = dbConn.getConn();
		
		int result = 0;
		WarningDataDao wdd = new WarningDataDao(conn);
		
		System.out.println("isSendWarn true");
		result = wdd.isSendWarn(1234, true);
		System.out.println("result: " + result);
		
		System.out.println("isSendWarn true");
		result = wdd.isSendWarn(1234, true);
		System.out.println("result: " + result);
		
		System.out.println("isSendWarn false");
		result = wdd.isSendWarn(1234, false);
		System.out.println("result: " + result);
		
//		WarningConfigDao wcd = new WarningConfigDao(conn);
//		FileRule fr = wcd.getRuleForFile("tptsvr.exe_flash_live.log.live");
//		
//		Map<Integer, ItemRule> itemRules = fr.getWarningRules();
//		for (Map.Entry<Integer, ItemRule> entry : itemRules.entrySet()) {
//			System.out.println("itil: "+ entry.getKey() + "|" + entry.getValue().getItilDesc());
//		}
//		
//		Map<String, SegmentRule> segRules = fr.getSegmentRules();
//		for (Map.Entry<String, SegmentRule> entry : segRules.entrySet()) {
//			System.out.println(entry.getValue().getCategory(null).getCategory());
//		}
//		
//		Set<String> exprs = fr.getExprs();
//		for (String expr : exprs) {
//			System.out.println("expr: " + expr);
//		}
	}
}
