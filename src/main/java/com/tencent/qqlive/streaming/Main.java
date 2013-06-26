package com.tencent.qqlive.streaming;

import java.sql.Connection;
import java.util.Set;

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.FileRule;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;


public class Main {
	public static void main(String[] args) throws Exception {
		DatabaseConnection dbConn = new DatabaseConnection("10.156.36.29", "3306", "root", "");
//		DatabaseConnection dbConn = new DatabaseConnection("10.130.2.16", "3306", "p2p", "!@#$qwerASDFzxcv");
		Connection conn = dbConn.getConn();
		
		WarningConfigDao wcd = new WarningConfigDao(conn);
		FileRule fr = wcd.getRuleForFile("tptsvr.exe_51.log.mobile");
		Set<String> exprs = fr.getExprs();
		for (String expr : exprs) {
			System.out.println("expr: " + expr);
		}
	}
}
