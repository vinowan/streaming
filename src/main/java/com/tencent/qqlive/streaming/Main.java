package com.tencent.qqlive.streaming;

import com.tencent.qqlive.streaming.util.IPInfo;
import com.tencent.qqlive.streaming.util.IPInfo.IPBlockInfo;

public class Main {
	public static void main(String[] args) throws Exception {
		IPInfo ipInfo = IPInfo.getInstance();
		IPBlockInfo blockInfo = ipInfo.getIPBlock("180.153.212.182");
		System.out.println(blockInfo.province);
		System.out.println(blockInfo.service);
		
//		String dbHost = "10.189.30.47";
//		String dbPort = "3325";
//		String dbUser = "p2p";
//		String dbPwd = "!@#$qwerASDFzxcv";
//		
//		DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort, dbUser, dbPwd);
//		HashMap<String, FileRule> fileRules = new HashMap<String, FileRule>();
//
//		try {
//			Connection conn = dbc.getConn();
//
//			WarningConfigDao wcd = new WarningConfigDao(conn);
//
//			List<String> statsFile = wcd.getAllStatsFiles();
//			for (String file : statsFile) {
//				FileRule fr = wcd.getRuleForFile(file);
//				fileRules.put(file, fr);
//
//				System.out.println("load file rule for: " + file);
//			}
//
//			conn.close();
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}
}
