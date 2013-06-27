package com.tencent.qqlive.streaming.spout;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.FileRule;
import com.tencent.qqlive.streaming.dao.ItemRule;
import com.tencent.qqlive.streaming.dao.LogEntry;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;
import com.tencent.qqlive.streaming.util.Config;
import com.tencent.qqlive.streaming.util.Utils;
import com.tencent.qqlive.streaming.util.ZkClient;

public class CollectorSpout implements IRichSpout {
	private static final long serialVersionUID = -2740279769470414180L;
	
	public static final String COMPONENT_NAME = "CollectorSpout";
	private static final String SPOUT_REGISTER_PATH = "/spout";
	
	private static final Logger logger = LoggerFactory
			.getLogger(CollectorSpout.class);
	
	private BlockingQueue<HinaEvent> messageQueue = new LinkedBlockingQueue<HinaEvent>(
			100000);

	private SpoutOutputCollector collector = null;

	private HinaSourceServer server = null;
	private ZkClient zkc = null;
	private Connection conn = null;
	
	private ScheduledExecutorService executor = null;
	
	private String dbHost = null;
	private String dbPort = null;
	private String dbUser = null;
	private String dbPwd = null;

	private Map<String, FileRule> fileRules = null;
	private CountDownLatch latch = null; // 用于标识初始化完成

	private SpoutStatics statics = null;

	public class DBConfigUpdater implements Runnable {

		public void run() {
			DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
					dbUser, dbPwd);
			fileRules = new HashMap<String, FileRule>();

			try {
				Connection conn = dbc.getConn();

				WarningConfigDao wcd = new WarningConfigDao(conn);

				List<String> statsFile = wcd.getAllStatsFiles();
				for (String file : statsFile) {
					FileRule fr = wcd.getRuleForFile(file);
					fileRules.put(file, fr);

					logger.info("load file rule for: " + file);
				}

				conn.close();

				latch.countDown();

			} catch (Exception e) {
				logger.error("failed to update db config: "
						+ Utils.stringifyException(e));
			}
		}

	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		
		executor = Executors.newSingleThreadScheduledExecutor();
		
		// stats
		statics = new SpoutStatics("CollectorSpout " + context.getThisTaskId());
		
//		int statsInterval = Integer.valueOf((String)conf.get("stats.interval"));
		int statsInterval = Config.getInt(conf, "stats.interval", 30);
		executor.scheduleAtFixedRate(statics, 0, statsInterval, TimeUnit.SECONDS);
		
		// load db config
		dbHost = (String) conf.get("db.host");
		dbPort = (String) conf.get("db.port");
		dbUser = (String) conf.get("db.user");
		dbPwd = (String) conf.get("db.password");
		
		if (dbHost == null || dbPort == null || dbUser == null
				|| dbPwd == null)
			throw new RuntimeException("failed to load db config");
		
		latch = new CountDownLatch(1);
		
//		int reloadInterval = Integer.valueOf((String)conf.get("reload.interval"));
		int reloadInterval = Config.getInt(conf, "reload.interval", 300);
		
		executor.scheduleAtFixedRate(new DBConfigUpdater(), 0, reloadInterval, TimeUnit.SECONDS);
		logger.info("waiting for load config from database: " + dbHost);
		
		// start server
		String zkHost = (String) conf.get("zk.host");
		String zkPath = (String) conf.get("zk.path");

		if (zkHost == null || zkPath == null)
			throw new RuntimeException("failed to read zookeeper config");

		try {
			// wait for db config
			if(!latch.await(10, TimeUnit.SECONDS)) 
				throw new RuntimeException("failed to load config from database: " + dbHost);
			
			server = new HinaSourceServer(messageQueue, statics);
			server.startServer();
			logger.info("start server " + server.getAddress() + ":"
					+ server.getPort());
			
			// register in zookeeper
			zkc = new ZkClient(zkHost);
			zkc.connect();
			
			zkc.register(zkPath + SPOUT_REGISTER_PATH,
					server.getAddress(), server.getPort());
			logger.info("register in zookeeper " + zkHost + " path " + zkPath
					+ SPOUT_REGISTER_PATH);
		} catch (Exception e) {
			logger.error("failed to connect to zookeeper: " + Utils.stringifyException(e));
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			if (zkc != null) 
				zkc.disconnect();
			
			
			if (conn != null)
				conn.close();
			
			if (server != null) 
				server.stopServer();
		} catch (Exception e) {
			logger.error("failed to stop server: " + e.toString());
		}	
	}

	public void activate() {
		// TODO Auto-generated method stub
	}

	public void deactivate() {
		// TODO Auto-generated method stub
	}

	public void nextTuple() {
		HinaEvent event = messageQueue.poll();
		if (event == null)
			return;
		
		statics.handlePacket.getAndIncrement();
		
		FileRule fr = fileRules.get(event.getCategory());
		if (fr == null) {
			statics.wrongStreamPacket.getAndIncrement();
			return;
		}
		
		LogEntry log = parseEvent(event);
		if (log == null 
				|| System.currentTimeMillis() - log.getTimestamp() > 30 * 60 * 1000 ) {
			statics.timeoutPacket.getAndIncrement();
			return;
		}
		
		for (Map.Entry<Integer, ItemRule> entry : fr.getWarningRules().entrySet()) {
			if (entry.getValue().validate(log)) {
				List<Object> tuple = new ArrayList<Object>();
				// first item is itil id
				tuple.add(entry.getKey());
				
				for (Map.Entry<String, String> field : log.getFields().entrySet()) {
					tuple.add(new String(field.getKey() + "=" + field.getValue()));
				}
				
				collector.emit(log.getCategory(), tuple);
				statics.incr(log.getCategory());				
			}
		}
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for (Map.Entry<String, FileRule> entry : fileRules.entrySet()) {
			List<String> fields = new ArrayList<String>();
			fields.add("itil");
			fields.addAll(entry.getValue().getExprs());
			
			// 每个file都是一个单独的stream
			declarer.declareStream(entry.getKey(), new Fields(fields));
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private LogEntry parseEvent(HinaEvent event) {
		LogEntry entry = new LogEntry();
		
		entry.setCategory(event.getCategory());
		
		Set<String> exprs = fileRules.get(event.getCategory()).getExprs();
		
		String body = new String(event.getBody());
		String[] items = body.split("\\s+");
		String timestamp = items[0] + " " + items[1];
		SimpleDateFormat formatter = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.S");
		Date date = null;
		try {
			date = formatter.parse(timestamp);
		} catch (ParseException e) {
			logger.warn("failed to parse timestamp: "
					+ Utils.stringifyException(e));
			return null;
		}
		entry.setTimestamp(date.getTime());
		
		for (int i = 2; i < items.length; i++) {
			String[] keyVal = items[i].split("\\s*=\\s*");
			if (keyVal.length != 2
					|| !exprs.contains(keyVal[0]))
				continue;
			
			entry.addField(keyVal[0], keyVal[1]);
		}
		
		return entry;
	}

}
