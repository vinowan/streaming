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
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.tencent.qqlive.streaming.dao.DatabaseConnection;
import com.tencent.qqlive.streaming.dao.FileRule;
import com.tencent.qqlive.streaming.dao.ItilRule;
import com.tencent.qqlive.streaming.dao.LogEntry;
import com.tencent.qqlive.streaming.dao.WarningConfigDao;
import com.tencent.qqlive.streaming.topology.StreamingTopology;
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
	
	private ScheduledExecutorService executor = null;
	
	private String dbHost = null;
	private String dbPort = null;
	private String dbUser = null;
	private String dbPwd = null;

	private AtomicReference<HashMap<String, FileRule>> fileRulesRef = null;
	private CountDownLatch latch = null; // 用于标识初始化完成

	private SpoutStatics statics = null;
	
	public class DBConfigUpdater implements Runnable {

		public void run() {
			DatabaseConnection dbc = new DatabaseConnection(dbHost, dbPort,
					dbUser, dbPwd);
			HashMap<String, FileRule> fileRules = new HashMap<String, FileRule>();

			try {
				Connection conn = dbc.getConn();

				WarningConfigDao wcd = new WarningConfigDao(conn);

				List<String> statsFiles = wcd.getAllStatsFiles();
				for (String file : statsFiles) {
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
		
		fileRulesRef = new AtomicReference<HashMap<String,FileRule>>();
		
		executor.scheduleAtFixedRate(new DBConfigUpdater(), 0, reloadInterval, TimeUnit.SECONDS);
		logger.info("waiting for load config from database: " + dbHost);
		
		// start server
		String zkHost = (String) conf.get("zk.host");
		String zkPath = (String) conf.get("zk.path");

		if (zkHost == null || zkPath == null)
			throw new RuntimeException("failed to read zookeeper config");

		try {
			// wait for db config
			if(!latch.await(60, TimeUnit.SECONDS)) 
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
		
		String log = "2013-07-02 14:15:00.002 [22446] [info] remote=171.123.52.86 qq=0 devid=B16A6B29-129B-4FB2-9379-5C1FB6898966 devtype=3 stick=1372679865974 ctick=1372745505662 ptick=512 cdnid=203 vodtype=3 playno=-1 playid=T00103WJHNb vodf=2 block=0 blocktime=0 blockb=-1 blocke=-1 seek=0 seektime=0 seekb=0 seeke=0 play=194 errcode=0 nettype=1 netstrength=-1 mcc=0 mnc=0 vodaddr=http://221.204.198.24/vlive.qqvideo.tc.qq.com/T00103WJHNb.p202.1.mp4?vkey=CDDA1D9FDEAC5E6EB231A829A486D2FE493E8D81908ADAFF025017BF26F5C5C8AB535A15B18CEBD7&sha=&level=3&br=200&fmt=hd&sdtfrom=v4000&platform=10103 app=2.3.0.2512 block_detail=no_play_block_info os_version=6.1.2 resolution=1536*2048 get_format_time=-1 get_url_time=-1 video_time=193 dev_model=iPad use_dlna=-1 play_source=1 net_ok=1 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=6a737968e7641030b355d48564437054 dray_release=-1 market_id=1 pay_status=-1 pay_status_ios=-2 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=hmc0z7kb4pldg4d mac=60:FE:C5:75:77:AE log_flag=51";
		messageQueue.offer(new HinaEvent(log.getBytes(), "tptsvr.exe_51.log.mobile"));
	}

	public void close() {
		try {
			if (zkc != null) 
				zkc.disconnect();
			
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
		
		FileRule fr = fileRulesRef.get().get(event.getCategory());
		if (fr == null) {
			statics.wrongStreamPacket.getAndIncrement();
			return;
		}
		
		LogEntry log = parseEvent(event);
//		if (log == null 
//				|| System.currentTimeMillis() - log.getTimestamp() > 30 * 60 * 1000 ) {
//			statics.timeoutPacket.getAndIncrement();
//			return;
//		}
		
		for (Map.Entry<Integer, ItilRule> entry : fr.getWarningRules().entrySet()) {
			if (entry.getValue().validate(log)) {
				List<Object> tuple = new ArrayList<Object>();
				// first item is category
				tuple.add(log.getCategory());
				logger.info("Category: " + log.getCategory());
				// second item is itil id
				tuple.add(entry.getKey());
				logger.info("Itil: " + entry.getKey());
				// third item is timestamp
				tuple.add(log.getTimestamp());
				logger.info("Timestamp: " + log.getTimestamp());
				
				List<String> body = new ArrayList<String>();
				for (Map.Entry<String, String> field : log.getFields().entrySet()) {
					body.add(new String(field.getKey() + "=" + field.getValue()));
				}
				
				tuple.add(Utils.join(body, "\t"));
				logger.info("Body: " + Utils.join(body, "\t"));
				
//				collector.emit(StreamingTopology.STREAM_ID, tuple);
				statics.emitStream.getAndIncrement();			
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
		declarer.declareStream(StreamingTopology.STREAM_ID, new Fields("category", "itil", "timestamp", "body"));
//		for (Map.Entry<String, FileRule> entry : fileRulesRef.get().entrySet()) {
//			List<String> fields = new ArrayList<String>();
//			fields.add("itil");
//			fields.add("timestamp");
//			fields.addAll(entry.getValue().getExprs());
//			
//			// 每个file都是一个单独的stream
//			declarer.declareStream(entry.getKey(), new Fields(fields));
//		}
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private LogEntry parseEvent(HinaEvent event) {
		LogEntry entry = new LogEntry();
		
		entry.setCategory(event.getCategory());
		
		Set<String> exprs = fileRulesRef.get().get(event.getCategory()).getExprs();
		
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
