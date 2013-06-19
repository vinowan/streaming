package com.tencent.qqlive.streaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.tencent.qqlive.streaming.util.ZkClient;

public class Main {
	public static void main(String[] args) throws Exception {
//		String dbUrl = "jdbc:mysql://10.156.36.29/p2pstatistic";
//		String username = "root";
//		String password = "";
//		
//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//		} catch (ClassNotFoundException e) {
//			System.err.println("failed to load jdbc driver");
//			System.exit(-1);
//		}
//		
//		try {
//			Connection conn = DriverManager.getConnection(dbUrl, username, password);
//			Statement statement = conn.createStatement();
//			ResultSet rs = statement.executeQuery("select * from t_liveconfig_info");
//			
//			while(rs.next()) {
//				System.out.println("result: " + rs.toString());
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		
		Properties prop = new Properties();
		try {
			prop.load(Thread.currentThread().
					getContextClassLoader().getResourceAsStream(args[0]));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		ZkClient zkc = new ZkClient((String)prop.get("zk.host"));
		BufferedReader reader = null;
		StringBuilder sb = new StringBuilder();
		try {
			reader = new BufferedReader(new FileReader(args[1]));
			String line = null;
			while((line = reader.readLine()) != null) {
				sb.append(line);
				sb.append("\n");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		zkc.connect();
		zkc.writeConf("/test-streaming/conf/stream", sb.toString());
		zkc.disconnect();
	}
}
