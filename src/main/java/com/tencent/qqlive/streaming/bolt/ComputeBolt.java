package com.tencent.qqlive.streaming.bolt;

import java.sql.Connection;
import java.util.Calendar;
import java.util.HashMap;
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
import com.tencent.qqlive.streaming.dao.ItilRule;
import com.tencent.qqlive.streaming.dao.SegmentRule;
import com.tencent.qqlive.streaming.dao.SegmentRule.Dimension;
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
		private String category = null;
		private Integer itil = null;
		
		public ID(String category, Integer itil) {
			this.category = category;
			this.itil = itil;
		}

		public String getCategory() {
			return category;
		}

		public Integer getItil() {
			return itil;
		}
		
		@Override
		public int hashCode() {
			int ret = 17;
			ret = 37*ret + category.hashCode();
			ret = 37*ret + itil.hashCode();
			
			return ret;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ID) {
				ID id = (ID)obj;
				return category.equals(id.category) && itil.equals(id.itil);
			}
			
			return false;
		}
	}
	// itil -> 计算表达式
	private AtomicReference<HashMap<ID, ElementaryArithmetic>> itilExprsRef = null;
	// itil -> 细分值 -> 计算表达式
	private AtomicReference<HashMap<ID, HashMap<Dimension, ElementaryArithmetic>>> itilSegExprsRef = null;

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
			HashMap<ID, HashMap<Dimension, ElementaryArithmetic>> itilSegExprs 
				= itilSegExprsRef.getAndSet(new HashMap<ID, HashMap<Dimension, ElementaryArithmetic>>());
			
			long timestamp = currentTs.get();
			timestamp = timestamp - timestamp % dumpInterval;
			
			DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
					dbUser, dbPwd);
			
			try {
				Connection conn = dbc.getConn();
				
				WarningDataDao wdd = new WarningDataDao(conn);
				
				int sqlNum = 0;
				long beginTs = System.currentTimeMillis();
				for (Map.Entry<ID, ElementaryArithmetic> entry : itilExprs.entrySet()) {
					String category = entry.getKey().getCategory();
					int itil = entry.getKey().getItil();
					
					FileRule fileRule = fileRulesRef.get().get(category);
					ItilRule itemRule = fileRule.getWarningRules().get(itil);
					
					double totalResult = entry.getValue().calcResult();
					totalResult = itemRule.zoom(totalResult);
					
					int totalCount = entry.getValue().getCount();
					
					wdd.insertItilMonitor(timestamp, itil, totalResult);
					sqlNum++;
					
					Calendar cal = Calendar.getInstance();
					cal.setTimeInMillis(timestamp);
					
					int hour = cal.get(Calendar.HOUR_OF_DAY);
//					System.out.println("-------hour:"+hour);
					// 如果不需要告警，则继续处理下一个itil					
					boolean status = itemRule.getHourMotion().validate(totalResult, hour);
//					System.out.println("-------status:"+status);
					int recovery = wdd.isSendWarn(itil, status);
//					System.out.println("-------recovery:"+recovery);
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
					
					// 仅当产生告警时做细分分析
					if (recovery <= 0)
						continue;
					
					HashMap<Dimension, ElementaryArithmetic> segExprs = itilSegExprs.get(entry.getKey());
					for (Map.Entry<Dimension, ElementaryArithmetic> subEntry : segExprs.entrySet()) {
						double splitResult = subEntry.getValue().calcResult();
						splitResult = itemRule.zoom(splitResult);
						int splitCount = subEntry.getValue().getCount();
						
						double contribRate = itemRule.calcContribRate(hour, splitResult, totalResult, splitCount, totalCount);
//						System.out.println("+++++" + String.format("%s:%s(%f,%f,%d,%f,%d)", subEntry.getKey().getDimension(), subEntry.getKey().getValue(),
//								contribRate, splitResult, splitCount, totalResult, totalCount));
						SegmentRule segRule = fileRule.getSegmentRules().get(subEntry.getKey().getDimension());
						if (contribRate > segRule.getContribMinRate()) {
							wdd.insertEMailWarning(timestamp, itil, itemRule.getItilDesc(), subEntry.getKey().getDimension(), subEntry.getKey().getValue(), 
									splitResult, totalResult, range.toString(), contribRate, itemRule.getReceiver());
							sqlNum++;
						}
					}
				}
				long endTs = System.currentTimeMillis();
				
				logger.info("dump " + sqlNum + " sql cost " + (endTs - beginTs));
				
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

		itilSegExprsRef = new AtomicReference<HashMap<ID, HashMap<Dimension, ElementaryArithmetic>>>();
		itilSegExprsRef.set(new HashMap<ID, HashMap<Dimension, ElementaryArithmetic>>());

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
		
		fileRulesRef = new AtomicReference<HashMap<String,FileRule>>();

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
		int initialDelay = dumpInterval - (int)(System.currentTimeMillis()/1000) % dumpInterval;
		
		executor.scheduleAtFixedRate(new DataDumper(), initialDelay, dumpInterval, TimeUnit.SECONDS);
	}

	public void execute(Tuple input) {
		String category = input.getString(0);
		
		statics.getStatics(category).inPacket.getAndIncrement();
		
		if (input.size() < 4) {
			statics.getStatics(category).wrongTuple.getAndIncrement();
			return;
		}
		
		
		int itil = input.getInteger(1);
		long timestamp = input.getLong(2);
		String body = input.getString(3);
		
//		System.out.println("-------category:" + category);
//		System.out.println("-------itil:" + itil);
//		System.out.println("-------timestamp:" + timestamp);
//		System.out.println("-------body:" + body);
		
		// get file rule
		FileRule fileRule = fileRulesRef.get().get(category);
		if (fileRule == null) {
			statics.getStatics(category).wrongCategory.getAndIncrement();
			return;
		}
		
		// update timestamp
		if (timestamp > currentTs.get())
			currentTs.set(timestamp);
		
		// parse body
		Map<String, String> itemValues = parseBody(body);

		// total
		ItilRule itemRule = fileRule.getWarningRules().get(itil);
		if (itemRule == null) {
			statics.getStatics(category).wrongItil.getAndIncrement();
			return;
		}
		
		ID id = new ID(category, itil);
		ElementaryArithmetic expr = itilExprsRef.get().get(id);
		if (expr == null) {
//			System.out.println("******itemName" + itemRule.getArithExpr().getItemName());
			expr = new ElementaryArithmetic(itemRule.getArithExpr()
					.getItemName());
			itilExprsRef.get().put(id, expr);
		}

		expr.compute(itemValues, itemRule.getItemRanges());

		// segment
		HashMap<Dimension, ElementaryArithmetic> segExprs = itilSegExprsRef
				.get().get(id);
		if (segExprs == null) {
			segExprs = new HashMap<Dimension, ElementaryArithmetic>();
			itilSegExprsRef.get().put(id, segExprs);
		}

		for (SegmentRule segRule : fileRule.getSegmentRules().values()) {
			Dimension dimension = segRule.getDimension(itemValues);
			if (dimension == null) {
				statics.getStatics(category).dimensionNull.getAndIncrement();
				continue;
			}
			
//			System.out.println("+++++++++" + dimension.toString());
			
			expr = segExprs.get(dimension);
			if (expr == null) {
				expr = new ElementaryArithmetic(itemRule.getArithExpr()
						.getItemName());
				segExprs.put(dimension, expr);
			}

			expr.compute(itemValues, itemRule.getItemRanges());
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
	
	private Map<String, String> parseBody(String body) {
		Map<String, String> itemValues = new HashMap<String, String>();
		
		String[] items = body.split("\\s+");
		for (String item : items) {
			String[] key_value = item.split("\\s*=\\s*", 2);
			if (key_value.length != 2)
				continue;
			
			itemValues.put(key_value[0], key_value[1]);
		}
		
		return itemValues;
	}
	
	public static void main(String[] args) {
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp - timestamp % 300;
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		
		System.out.println(cal.get(Calendar.HOUR_OF_DAY));
	}
}
