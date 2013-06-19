package com.tencent.qqlive.streaming.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.tencent.qqlive.streaming.spout.LogEntry;
import com.tencent.qqlive.streaming.util.StringUtils;
import com.tencent.qqlive.streaming.util.ZkClient;

public class MultiDevVodCountBolt implements IRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(MultiDevVodCountBolt.class);
	
	private OutputCollector collector = null;
	private Map conf = null;
	private TopologyContext context = null;
	
	private ConcurrentMap<LogEntry, Integer> results = null;
	private ScheduledExecutorService executor = null;
	private ZkClient zkc = null;
	private Connection conn = null;
	
	private String dbHost = null;
	private String dbPort = null;
	private String dbUser = null;
	private String dbPassword = null;
	
	private int writeInterval = 0;
	
	private BoltStatics statics = null;
	
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.conf = conf;
		this.collector = collector;
		this.context = context;
		
		statics = new BoltStatics();
		
		results = new ConcurrentHashMap<LogEntry, Integer>();
		
		String zkHost = (String)conf.get("zk.host");
		String zkPath = (String)conf.get("zk.path");
		String reloadIntervalStr = (String)conf.get("reload.interval");
		String writeIntervalStr = (String)conf.get("db.write.interval");
		
		if (zkHost == null || zkPath == null 
				|| reloadIntervalStr == null || writeIntervalStr == null)
			throw new RuntimeException("failed to read config");
		
		writeInterval = Integer.valueOf(writeIntervalStr);
		
		try {
			zkc = new ZkClient(zkHost);
			zkc.connect();
			
			logger.info("connect to zookeeper " + zkHost);
			
			executor = Executors.newScheduledThreadPool(2);
//			executor.scheduleAtFixedRate(new Runnable() {
//
//				public void run() {
//					// load db conf
//					String configString;
//					try {
//						configString = zkc.readConf(MultiDevVodCountBolt.this.conf.get("zk.path") + Config.CONFIG_FILE_PATH);
//					} catch (Exception ex) {
//						logger.error("failed to load config from zookeepr: " + StringUtils.stringifyException(ex));
//						return;
//					}
//					logger.info("load config from zookeeper: " + configString);
//					
//					Config config = new Config(configString);
//					dbHost = config.get("db.host");
//					dbPort = config.get("db.port");
//					dbUser = config.get("db.user");
//					dbPassword = config.get("db.password");
					
					dbHost = (String)conf.get("db.host");
					dbPort = (String)conf.get("db.port");
					dbUser = (String)conf.get("db.user");
					dbPassword = (String)conf.get("db.password");
		
					if (dbHost == null || dbPort == null 
							|| dbUser == null || dbPassword == null)
						throw new RuntimeException("failed to load db config");				
//				}
//							
//			}, 0, Integer.valueOf(reloadIntervalStr), TimeUnit.SECONDS);
			
			logger.info("schedule db writer at interval " + writeIntervalStr);
			
			int initialDelay = writeInterval - (int)(System.currentTimeMillis()/1000) % writeInterval;
			executor.scheduleAtFixedRate(new Runnable() {

				public void run() {
					
					logger.info("Bolt Statics: " + MultiDevVodCountBolt.this.context.getThisTaskId());
					logger.info(statics.toStr());
					
					statics.reset();
					
					// connect to db
					String dbUrl = String.format("jdbc:mysql://%s:%s/", dbHost, dbPort);
					try {
						Class.forName("com.mysql.jdbc.Driver");
					} catch (ClassNotFoundException e) {
						throw new RuntimeException("failed to load mysql jdbc driver");
					}
					
					try {
						conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
						PreparedStatement statement = conn.prepareStatement("INSERT INTO d_live_manage.t_multi_dev_vod_view(album, playid, devtype, timestamp, view) VALUES(?, ?, ?, ?, ?);");
						
						for(Map.Entry<LogEntry, Integer> entry : results.entrySet()) {
							statement.setString(1, entry.getKey().getAlbum());
							statement.setString(2, entry.getKey().getPlayid());
							statement.setString(3, entry.getKey().getDevtype());
							statement.setTimestamp(4, new Timestamp(entry.getKey().getTimestamp()));
							statement.setInt(5, entry.getValue());
							
							statement.executeUpdate();
							
							logger.info("execute sql: " + statement.toString());
						}
						
						results.clear();
						conn.close();
					} catch (SQLException e) {
						logger.error("failed to execute sql: " + StringUtils.stringifyException(e));
					}
				}
							
			}, initialDelay, Integer.valueOf(writeIntervalStr), TimeUnit.SECONDS);
			
		} catch (Exception e) {
			logger.error("failed to open bolt: " + StringUtils.stringifyException(e));
		}
	}

	public void execute(Tuple input) {
		statics.inPacket.getAndIncrement();
		
		long timestamp = input.getLong(0);
		String album = input.getString(1);
		String playid = input.getString(2);
		String devtype = input.getString(3);
		
//		logger.info("recv: " + timestamp + " " + album + " " + playid + " " + devtype);
		
		timestamp = timestamp - timestamp % (writeInterval * 1000);
		
		LogEntry entry = new LogEntry();
		entry.setAlbum(album);
		entry.setPlayid(playid);
		entry.setDevtype(devtype);
		entry.setTimestamp(timestamp);
		
		Integer count = results.putIfAbsent(entry, 1);
		if (count != null) {
			results.replace(entry, count + 1);
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void main(String[] args) throws Exception {
		int initialDelay = 300 - (int)(System.currentTimeMillis()/1000) % 300;
		System.out.println(initialDelay);
	}
}
