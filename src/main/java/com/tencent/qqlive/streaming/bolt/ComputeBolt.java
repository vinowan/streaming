package com.tencent.qqlive.streaming.bolt;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.FileRule;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;
import com.tencent.qqlive.streaming.util.Config;
import com.tencent.qqlive.streaming.util.Utils;

public class ComputeBolt implements IRichBolt {
	private static final long serialVersionUID = -5870171613070444345L;
	
	public static final String COMPONENT_NAME = "ComputeBolt";
	
	private static final Logger logger = LoggerFactory
			.getLogger(ComputeBolt.class);
	
	private ScheduledExecutorService executor = null;
	
	private String dbHost = null;
	private String dbPort = null;
	private String dbUser = null;
	private String dbPwd = null;
	
	private Map<String, FileRule> fileRules = null;
	private CountDownLatch latch = null; // 用于标识初始化完成
	
	private BoltStatics statics = null;
	
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
	
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		
		executor = Executors.newSingleThreadScheduledExecutor();
		
		// stats
		statics = new BoltStatics("ComputeBolt " + context.getThisTaskId());
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
		
		int reloadInterval = Config.getInt(conf, "reload.interval", 300);
		
		executor.scheduleAtFixedRate(new DBConfigUpdater(), 0, reloadInterval, TimeUnit.SECONDS);
		logger.info("waiting for load config from database: " + dbHost);
		
		try {
			if(!latch.await(10, TimeUnit.SECONDS)) 
				throw new RuntimeException("failed to load config from database: " + dbHost);
		} catch (InterruptedException e) {
			logger.error("failed to load config from database: " + Utils.stringifyException(e));
			throw new RuntimeException("failed to load config from database: " + dbHost);
		}
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
