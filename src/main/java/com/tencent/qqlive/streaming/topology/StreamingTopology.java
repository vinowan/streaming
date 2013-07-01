package com.tencent.qqlive.streaming.topology;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;

import com.tencent.qqlive.streaming.bolt.ComputeBolt;
import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;
import com.tencent.qqlive.streaming.spout.CollectorSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StreamingTopology {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: conffile");
			System.exit(-1);
		}
		
		Properties prop = new Properties();
		prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(args[0]));
		
		Config conf = new Config();
		for (String key : prop.stringPropertyNames()) {
			conf.put(key, prop.getProperty(key));
		}
		
		conf.setNumWorkers(1);
		conf.setNumAckers(0);
		
		String dbHost = (String) conf.get("db.host");
		String dbPort = (String) conf.get("db.port");
		String dbUser = (String) conf.get("db.user");
		String dbPwd = (String) conf.get("db.password");
		
		if (dbHost == null || dbPort == null || dbUser == null
				|| dbPwd == null)
			throw new RuntimeException("failed to load db config");
		
		DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
				dbUser, dbPwd);
		
		Connection conn = dbc.getConn();

		WarningConfigDao wcd = new WarningConfigDao(conn);
		
		List<String> statsFile = wcd.getAllStatsFiles();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(CollectorSpout.COMPONENT_NAME, new CollectorSpout(), 1);
		BoltDeclarer declarer = builder.setBolt(ComputeBolt.COMPONENT_NAME, new ComputeBolt(), 1);
		for (String stream : statsFile) {
			declarer.fieldsGrouping(CollectorSpout.COMPONENT_NAME, stream, new Fields("itil"));
		}
		
		StormSubmitter.submitTopology("Streaming", conf, builder.createTopology());
	}
	
}
