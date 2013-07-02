package com.tencent.qqlive.streaming.bolt;

import java.sql.Connection;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.ElementaryArithmetic;
import com.tencent.qqlive.streaming.dao.FileRule;
import com.tencent.qqlive.streaming.dao.HourMotion;
import com.tencent.qqlive.streaming.dao.ItemRule;
import com.tencent.qqlive.streaming.dao.SegmentRule;
import com.tencent.qqlive.streaming.dao.SegmentRule.Category;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;
import com.tencent.qqlive.streaming.dao.WarningDataDao;
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

	private AtomicReference<HashMap<String, FileRule>> fileRulesRef = null;
	private CountDownLatch latch = null; // 用于标识初始化完成

	private BoltStatics statics = null;
	
	private int dumpInterval = 300;	
	// 日志的最新时间戳
	private AtomicLong currentTs = null;
	
	public static class ID {
		private String stream = null;
		private Integer itil = null;
		
		public ID(String stream, Integer itil) {
			this.stream = stream;
			this.itil = itil;
		}

		public String getStream() {
			return stream;
		}

		public Integer getItil() {
			return itil;
		}
		
		@Override
		public int hashCode() {
			int ret = 17;
			ret = 37*ret + stream.hashCode();
			ret = 37*ret + itil.hashCode();
			
			return ret;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ID) {
				ID id = (ID)obj;
				return stream.equals(id.stream) && itil.equals(id.itil);
			}
			
			return false;
		}
	}
	// itil -> 计算表达式
	private AtomicReference<HashMap<ID, ElementaryArithmetic>> itilExprsRef = null;
	// itil -> 细分值 -> 计算表达式
	private AtomicReference<HashMap<ID, HashMap<Category, ElementaryArithmetic>>> itilSegExprsRef = null;

	public class DBConfigUpdater implements Runnable {

		public void run() {
			DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
					dbUser, dbPwd);
			HashMap<String, FileRule> fileRules = new HashMap<String, FileRule>();

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

				fileRulesRef.set(fileRules);

				latch.countDown();

			} catch (Exception e) {
				logger.error("failed to update db config: "
						+ Utils.stringifyException(e));
			}
		}
	}

	public class DataDumper implements Runnable {

		public void run() {
			HashMap<ID, ElementaryArithmetic> itilExprs 
				= itilExprsRef.getAndSet(new HashMap<ID, ElementaryArithmetic>());
			HashMap<ID, HashMap<Category, ElementaryArithmetic>> itilSegExprs 
				= itilSegExprsRef.getAndSet(new HashMap<ID, HashMap<Category, ElementaryArithmetic>>());
			
			long timestamp = currentTs.get();
			timestamp = timestamp - timestamp % dumpInterval;
			
			DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
					dbUser, dbPwd);
			
			try {
				Connection conn = dbc.getConn();
				
				WarningDataDao wdd = new WarningDataDao(conn);
				
				for (Map.Entry<ID, ElementaryArithmetic> entry : itilExprs.entrySet()) {
					String stream = entry.getKey().getStream();
					int itil = entry.getKey().getItil();
					
					FileRule fileRule = fileRulesRef.get().get(stream);
					ItemRule itemRule = fileRule.getWarningRules().get(itil);
					
					double totalResult = entry.getValue().calcResult();
					totalResult = itemRule.zoom(totalResult);
					
					int totalCount = entry.getValue().getCount();
					
					wdd.insertItilMonitor(timestamp, itil, totalResult);
					
					Calendar cal = Calendar.getInstance();
					cal.setTimeInMillis(timestamp);
					
					int hour = cal.get(Calendar.HOUR_OF_DAY);
					// 如果不需要告警，则继续处理下一个itil
//					if(!itemRule.getHourMotion().validate(totalResult, hour))
//						continue;
					
					boolean status = itemRule.getHourMotion().validate(totalResult, hour);
					int recovery = wdd.isSendWarn(itil, status);
					String recoveryDesc = "";
					if (recovery > 0) {
						recoveryDesc = "告警通知";
					} else if (recovery < 0) {
						recoveryDesc = "告警恢复";
					} else {
						// recovery为0，不告警
						continue;
					}
					
					HourMotion.Range range = itemRule.getHourMotion().getRange(hour);
					wdd.insertSMSWarning(timestamp, itil, itemRule.getItilDesc(), itemRule.getBusiness(), 
							recoveryDesc, totalResult, range.toString(), itemRule.getReceiver());
					
					HashMap<Category, ElementaryArithmetic> segExprs = itilSegExprs.get(entry.getKey());
					for (Map.Entry<Category, ElementaryArithmetic> subEntry : segExprs.entrySet()) {
						double splitResult = subEntry.getValue().calcResult();
						splitResult = itemRule.zoom(splitResult);
						int splitCount = subEntry.getValue().getCount();
						
						double contribRate = itemRule.calcContribRate(hour, splitResult, totalResult, splitCount, totalCount);
						SegmentRule segRule = fileRule.getSegmentRules().get(subEntry.getKey().getCategory());
						if (contribRate > segRule.getContribMinRate()) {
							wdd.insertEMailWarning(timestamp, itil, itemRule.getItilDesc(), subEntry.getKey().getCategory(), subEntry.getKey().getValue(), 
									splitResult, totalResult, range.toString(), contribRate, itemRule.getReceiver());
						}
					}
				}
				
			} catch (Exception e) {
				logger.error("failed to write data into db: "
						+ Utils.stringifyException(e));
			}
		}
	}

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		currentTs = new AtomicLong();
		
		itilExprsRef = new AtomicReference<HashMap<ID, ElementaryArithmetic>>();
		itilExprsRef.set(new HashMap<ID, ElementaryArithmetic>());

		itilSegExprsRef = new AtomicReference<HashMap<ID, HashMap<Category, ElementaryArithmetic>>>();
		itilSegExprsRef.set(new HashMap<ID, HashMap<Category, ElementaryArithmetic>>());

		executor = Executors.newScheduledThreadPool(2);

		// stats
		statics = new BoltStatics("ComputeBolt " + context.getThisTaskId());
		int statsInterval = Config.getInt(conf, "stats.interval", 30);
		executor.scheduleAtFixedRate(statics, 0, statsInterval,
				TimeUnit.SECONDS);

		// load db config
		dbHost = (String) conf.get("db.host");
		dbPort = (String) conf.get("db.port");
		dbUser = (String) conf.get("db.user");
		dbPwd = (String) conf.get("db.password");

		if (dbHost == null || dbPort == null || dbUser == null || dbPwd == null)
			throw new RuntimeException("failed to load db config");

		latch = new CountDownLatch(1);

		int reloadInterval = Config.getInt(conf, "reload.interval", 300);

		executor.scheduleAtFixedRate(new DBConfigUpdater(), 0, reloadInterval,
				TimeUnit.SECONDS);
		logger.info("waiting for load config from database: " + dbHost);

		try {
			if (!latch.await(10, TimeUnit.SECONDS))
				throw new RuntimeException(
						"failed to load config from database: " + dbHost);
		} catch (InterruptedException e) {
			logger.error("failed to load config from database: "
					+ Utils.stringifyException(e));
			throw new RuntimeException("failed to load config from database: "
					+ dbHost);
		}
		
		dumpInterval = Config.getInt(conf, "dump.interval", 300);
		executor.scheduleAtFixedRate(new DataDumper(), 0, dumpInterval, TimeUnit.SECONDS);
	}

	public void execute(Tuple input) {
		// get stream id
		String stream = input.getSourceStreamId();
		FileRule fileRule = fileRulesRef.get().get(stream);
		if (fileRule == null) {
			statics.wrongStream.getAndIncrement();
			return;
		}
		
		// parse tuple body
		List<Object> values = input.getValues();
		if (values.size() < 3) {
			statics.wrongTuple.getAndIncrement();
			return;
		}

		Map<String, String> itemValues = new HashMap<String, String>();

		Iterator<Object> it = values.iterator();
		Integer itil = (Integer)it.next();
		Long timestamp = (Long)it.next();
		if (timestamp > currentTs.get())
			currentTs.set(timestamp);
		
		while (it.hasNext()) {
			String[] key_value = ((String) it.next()).split("\\s*=\\s*");
			if (key_value.length != 2)
				continue;

			itemValues.put(key_value[0], key_value[1]);
		}

		// total
		ItemRule itemRule = fileRule.getWarningRules().get(itil);
		if (itemRule == null) {
			statics.wrongItil.getAndIncrement();
			return;
		}
		
		ElementaryArithmetic expr = itilExprsRef.get().get(itil);
		if (expr == null) {
			expr = new ElementaryArithmetic(itemRule.getArithExpr()
					.getItemName());
			itilExprsRef.get().put(new ID(stream, itil), expr);
		}

		expr.compute(itemValues);

		// segment
		HashMap<Category, ElementaryArithmetic> segExprs = itilSegExprsRef
				.get().get(itil);
		if (segExprs == null) {
			segExprs = new HashMap<Category, ElementaryArithmetic>();
			itilSegExprsRef.get().put(new ID(stream, itil), segExprs);
		}

		for (SegmentRule segRule : fileRule.getSegmentRules().values()) {
			Category category = segRule.getCategory(itemValues);
			if (category == null) {
				statics.categoryNull.getAndIncrement();
				continue;
			}

			expr = segExprs.get(category);
			if (expr == null) {
				expr = new ElementaryArithmetic(itemRule.getArithExpr()
						.getItemName());
				segExprs.put(category, expr);
			}

			expr.compute(itemValues);
		}
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
	
	public static void main(String[] args) {
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp - timestamp % 300;
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		
		System.out.println(cal.get(Calendar.HOUR_OF_DAY));
	}
}
