package com.tencent.qqlive.streaming;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.FileRule;
import com.tencent.qqlive.streaming.dao.SegmentRule;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;
import com.tencent.qqlive.streaming.util.ConfigUtils;

public class Main {
	public static void main(String[] args) throws Exception {
		String dbHost = "10.189.30.47";
		String dbPort = "3325";
		String dbUser = "p2p";
		String dbPwd = "!@#$qwerASDFzxcv";
		
		DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort, dbUser, dbPwd);

		try {
			Connection conn = dbc.getConn();

			WarningConfigDao wcd = new WarningConfigDao(conn);
			
			HashMap<String, String> verToPlt = wcd.getVersionPlatformMap();
			
			ConfigUtils.verToPltRef.set(verToPlt);
			
			conn.close();
			
			SegmentRule.Segment seg = new SegmentRule.Segment();
			
			seg.setItemName("version");
			seg.setOperation(SegmentRule.Segment.OPERATION_TYPE_GET_PLATFORM);
			seg.setDimension("ƽ̨");
			
			System.out.println(seg.getValue("3.3.3.3"));
			System.out.println(seg.getValue("2.1.2"));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
