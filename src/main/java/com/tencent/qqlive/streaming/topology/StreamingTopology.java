package com.tencent.qqlive.streaming.topology;

import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tencent.qqlive.streaming.bolt.ComputeBolt;
import com.tencent.qqlive.streaming.spout.CollectorSpout;

public class StreamingTopology {
	public static final String STREAM_ID = "streaming"; 
	
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
		
//		String dbHost = (String) conf.get("db.host");
//		String dbPort = (String) conf.get("db.port");
//		String dbUser = (String) conf.get("db.user");
//		String dbPwd = (String) conf.get("db.password");
//		
//		if (dbHost == null || dbPort == null || dbUser == null
//				|| dbPwd == null)
//			throw new RuntimeException("failed to load db config");
//		
//		DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
//				dbUser, dbPwd);
//		
//		Connection conn = dbc.getConn();
//
//		WarningConfigDao wcd = new WarningConfigDao(conn);
//		
//		List<String> statsFile = wcd.getAllStatsFiles();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(CollectorSpout.COMPONENT_NAME, new CollectorSpout(), 1);
		builder.setBolt(ComputeBolt.COMPONENT_NAME, new ComputeBolt(), 1)
			.fieldsGrouping(CollectorSpout.COMPONENT_NAME, STREAM_ID, new Fields("itil"));
		
		
//		StormSubmitter.submitTopology("Streaming", conf, builder.createTopology());
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Streaming", conf, builder.createTopology());
		Thread.currentThread().sleep(10000);
		cluster.shutdown();
	}
	
}
