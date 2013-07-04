package com.tencent.qqlive.streaming.topology;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tencent.qqlive.streaming.bolt.ComputeBolt;
import com.tencent.qqlive.streaming.spout.CollectorSpout;

public class StreamingTopology {
	public static final String STREAM_ID = "streaming";
	
	private static Logger logger = LoggerFactory.getLogger(StreamingTopology.class);
	
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
		
		conf.setNumWorkers(12);
		conf.setNumAckers(0);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(CollectorSpout.COMPONENT_NAME, new CollectorSpout(), 8);
		builder.setBolt(ComputeBolt.COMPONENT_NAME, new ComputeBolt(), 16)
			.fieldsGrouping(CollectorSpout.COMPONENT_NAME, STREAM_ID, new Fields("itil"));
		
		String mode = (String)conf.get("system.mode");
		if (mode == null 
				|| mode.equals("")) {
			mode = "distribute";
		}
		
		if (mode.equalsIgnoreCase("distribute")) {
			logger.info("topology running in distribute mode");
			
			StormSubmitter.submitTopology("Streaming", conf, builder.createTopology());
		} else {
			logger.info("topology running in local mode");
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Streaming", conf, builder.createTopology());
			Thread.sleep(70000);
			cluster.shutdown();
		}	
	}
	
}