package com.tencent.qqlive.streaming.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
	public static final String DRIVER = "com.mysql.jdbc.Driver";
	
	private String host = null;
	private String port = null;
	private String user = null;
	private String pwd = null;
	
	private Connection conn = null;
	
	public DatabaseConnection(String host, String port, String user, String pwd) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.pwd = pwd;
	}
	
	public Connection getConn() throws ClassNotFoundException, SQLException {
		if (conn != null) 
			return conn;
		
		String dbUrl = String.format("jdbc:mysql://%s:%s/?useUnicode=true&characterEncoding=GBK&autoReconnect=true", host, port);
		Class.forName(DRIVER);
		conn = DriverManager.getConnection(dbUrl, user, pwd);	
		
		return conn;
	}
	
	public void close() throws SQLException {
		if (conn != null) {	
			conn.close();
		}
	}
}
