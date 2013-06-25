package com.tencent.qqlive.streaming;

import java.sql.Connection;

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.WarningDataDao;


public class Main {
	public static void main(String[] args) throws Exception {
		DatabaseConnection dbConn = new DatabaseConnection("10.156.36.29", "3306", "root", "");
//		DatabaseConnection dbConn = new DatabaseConnection("10.130.2.16", "3306", "p2p", "!@#$qwerASDFzxcv");
		Connection conn = dbConn.getConn();
		
		WarningDataDao wdd = new WarningDataDao(conn);
		
		wdd.insertEMailWarning(System.currentTimeMillis(), 1, "1", "catgory", "value", 1.1, 2.2, "[1,100]", 10);
	}
}
