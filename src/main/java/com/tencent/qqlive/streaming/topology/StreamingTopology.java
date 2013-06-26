package com.tencent.qqlive.streaming.topology;

import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

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
		
		TopologyBuilder builder = new TopologyBuilder();
		
		StormSubmitter.submitTopology("Streaming", conf, builder.createTopology());
	}
	
}
