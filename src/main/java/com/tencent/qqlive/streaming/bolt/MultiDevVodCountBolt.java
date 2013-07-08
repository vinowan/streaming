package com.tencent.qqlive.streaming.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.tencent.qqlive.streaming.spout.LogEntry;
import com.tencent.qqlive.streaming.util.Utils;
import com.tencent.qqlive.streaming.util.ZkClient;

public class MultiDevVodCountBolt implements IRichBolt {
	private static final long serialVersionUID = 8541321114425153335L;

	private static final Logger logger = LoggerFactory.getLogger(MultiDevVodCountBolt.class);
	
	private OutputCollector collector = null;
	private Map conf = null;
	private TopologyContext context = null;
	
//	private ConcurrentMap<LogEntry, Integer> results = null;
	private AtomicReference<HashMap<LogEntry, Integer>> resultsRef = null;
	
	private ScheduledExecutorService executor = null;
	private ZkClient zkc = null;
	private Connection conn = null;
	
	private String dbHost = null;
	private String dbPort = null;
	private String dbUser = null;
	private String dbPassword = null;
	
	private int writeInterval = 0;
	
	private BoltStatics statics = null;
	
	private AtomicLong currTs = null;
	
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.conf = conf;
		this.collector = collector;
		this.context = context;
		
		currTs = new AtomicLong();
		
		statics = new BoltStatics();
		
		resultsRef = new AtomicReference<HashMap<LogEntry, Integer>>();
		resultsRef.set(new HashMap<LogEntry, Integer>());
		
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
					
					dbHost = (String)conf.get("data.db.host");
					dbPort = (String)conf.get("data.db.port");
					dbUser = (String)conf.get("data.db.user");
					dbPassword = (String)conf.get("data.db.password");
		
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
					
					HashMap<LogEntry, Integer> results = resultsRef.getAndSet(new HashMap<LogEntry, Integer>());
					// connect to db
					String dbUrl = String.format("jdbc:mysql://%s:%s/", dbHost, dbPort);
					try {
						Class.forName("com.mysql.jdbc.Driver");
					} catch (ClassNotFoundException e) {
						throw new RuntimeException("failed to load mysql jdbc driver");
					}
					
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyyMMdd");
					String dateStr = sdf.format(new Date(currTs.get()));
					
					try {
						conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
						PreparedStatement statement = conn.prepareStatement("INSERT INTO d_real_time_statis_data.t_multi_dev_vod_view_" + dateStr + "(album, playid, devtype, timestamp, view) VALUES(?, ?, ?, ?, ?);");
						
						for(Map.Entry<LogEntry, Integer> entry : results.entrySet()) {
//							if (entry.getValue() <= 5)
//								continue;
							
							statement.setString(1, entry.getKey().getAlbum());
							statement.setString(2, entry.getKey().getPlayid());
							statement.setString(3, entry.getKey().getDevtype());
							statement.setTimestamp(4, new Timestamp(entry.getKey().getTimestamp()));
							statement.setInt(5, entry.getValue());
							
							statement.executeUpdate();
							
							logger.info("execute sql: " + statement.toString());
						}
										
						conn.close();
					} catch (SQLException e) {
						logger.error("failed to execute sql: " + Utils.stringifyException(e));
					}
				}
							
			}, initialDelay, Integer.valueOf(writeIntervalStr), TimeUnit.SECONDS);
			
		} catch (Exception e) {
			logger.error("failed to open bolt: " + Utils.stringifyException(e));
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
		
		if (currTs.get() < timestamp) 
			currTs.set(timestamp);
		
		LogEntry entry = new LogEntry();
		entry.setAlbum(album);
		entry.setPlayid(playid);
		entry.setDevtype(devtype);
		entry.setTimestamp(timestamp);
		
		Integer count = resultsRef.get().get(entry);
		if (count == null) {
			count = new Integer(0);
			resultsRef.get().put(entry, count);
		}
		resultsRef.get().put(entry, count + 1);
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
		SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyyMMdd");
		String dateStr = sdf.format(new Date(System.currentTimeMillis()));
		String sql = "INSERT INTO d_real_time_statis_data.t_multi_dev_vod_view_" + dateStr + "(album, playid, devtype, timestamp, view) VALUES(?, ?, ?, ?, ?);";
		System.out.println(sql);
	}
}
