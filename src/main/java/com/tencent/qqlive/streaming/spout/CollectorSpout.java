package com.tencent.qqlive.streaming.spout;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
		
//		String log = null;
//		log = "2013-07-02 14:15:00.031 [22446] [info] remote=27.13.125.113 qq=924986105 devid=636FB517-0E7C-4E5D-83BF-CA222F49C2C1 devtype=4 stick=1372744894047 ctick=1372744936708 ptick=2204 cdnid=-1 vodtype=1 playno=-1 playid=n0010X6cgtp vodf=2 block=2 blocktime=9584 blockb=1372745304514 blocke=1372745310914 seek=62 seektime=55157 seekb=1372745635370 seeke=1372745635569 play=591 errcode=-11819 nettype=1 netstrength=-1 mcc=460 mnc=1 vodaddr=http://113.207.98.26/vlive.qqvideo.tc.qq.com/n0010X6cgtp.p202.1.mp4?vkey=53F60CEB3706BFE6F54D6A76E605221FF7FF061EA886BA9A6B66BCBB72105434E3B64A2E866AC1E5&sha=&level=3&br=200&fmt=hd&sdtfrom=v3000&platform=10403 app=2.2.1.3326 block_detail=[1202600,3185],[1238374,6400] os_version=6.1.3 resolution=640*960 get_format_time=-1 get_url_time=-1 video_time=5379 dev_model=iPhone use_dlna=-1 play_source=2 net_ok=1 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=4ce487a49ff01030b355d48564437054 dray_release=-1 market_id=1 pay_status=-1 pay_status_ios=0 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=5w4vn5eqtmj5i1n mac=no_mac log_flag=51";
//		messageQueue.offer(new HinaEvent(log.getBytes(), "tptsvr.exe_51.log.mobile"));
//		log = "2013-07-02 14:15:00.174 [22446] [info] remote=119.53.200.84 qq=0 devid=411DF926-4FC6-4662-88F5-1B604E62B66E devtype=4 stick=1372688371185 ctick=1372743409439 ptick=1634 cdnid=-1 vodtype=2 playno=-1 playid=m00126k7qhn vodf=10203 block=2 blocktime=1656 blockb=1372743810674 blocke=1372743811473 seek=1 seektime=0 seekb=1372743804025 seeke=1372743809074 play=2133 errcode=0 nettype=1 netstrength=-1 mcc=460 mnc=1 vodaddr=http://122.143.6.19/vlive.qqvideo.tc.qq.com/m00126k7qhn.p203.1.mp4?vkey=8ED0731007702272FCDE21C9853FCA43DC559F7E85D6F97BD95ACAE16277D2AF8F502FCBF301BAD6&sha=&level=3&br=200&fmt=sd&sdtfrom=v3000&platform=10403 app=2.2.1.3326 block_detail=[90800,857],[462240,799] os_version=5.0.1 resolution=640*960 get_format_time=-1 get_url_time=-1 video_time=2696 dev_model=iPhone use_dlna=-1 play_source=1 net_ok=-65535 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=0138615ee3751030b355d48564437054 dray_release=-1 market_id=2 pay_status=-1 pay_status_ios=-2 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=othfgnhftzxur65 mac=no_mac log_flag=51";
//		messageQueue.offer(new HinaEvent(log.getBytes(), "tptsvr.exe_51.log.mobile"));
//		log = "2013-07-02 14:15:00.245 [22446] [info] remote=219.155.59.207 qq=0 devid=863840F9-E32F-434F-B7F8-DCBE5182C657 devtype=4 stick=1372743667948 ctick=1372743696590 ptick=2610 cdnid=-1 vodtype=2 playno=-1 playid=u0012bs4u28 vodf=10202 block=0 blocktime=0 blockb=-1 blocke=-1 seek=12 seektime=0 seekb=1372745460963 seeke=1372745461961 play=1993 errcode=0 nettype=1 netstrength=-1 mcc=234 mnc=10 vodaddr=http://182.118.11.2/vlive.qqvideo.tc.qq.com/u0012bs4u28.p202.1.mp4?vkey=A790DD67B3F1450EFBE34E5B33AF5D308ABD6FDF59EFC4AC40B80781F4B644BE01E227E4682DF129&sha=&level=3&br=200&fmt=hd&sdtfrom=v3000&platform=10403 app=2.2.1.3326 block_detail=no_play_block_info os_version=6.1.4 resolution=640*1136 get_format_time=-1 get_url_time=-1 video_time=2577 dev_model=iPhone use_dlna=-1 play_source=1 net_ok=-65535 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=c058c32227671031b355abcd0b13a80a dray_release=-1 market_id=1 pay_status=-1 pay_status_ios=-2 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=c7ffkszrd27c4d4 mac=no_mac log_flag=51";
//		messageQueue.offer(new HinaEvent(log.getBytes(), "tptsvr.exe_51.log.mobile"));
//		log = "2013-07-02 14:15:00.325 [22446] [info] remote=119.6.119.29 qq=0 devid=F5397A3E-49B9-4CC9-B396-C94FA5D5904C devtype=4 stick=1372743918301 ctick=1372745108654 ptick=887 cdnid=-1 vodtype=2 playno=-1 playid=a0010P6OOWO vodf=10202 block=0 blocktime=0 blockb=1372745697542 blocke=-1 seek=105 seektime=16793 seekb=1372745686501 seeke=1372745688743 play=555 errcode=-1 nettype=1 netstrength=-1 mcc=460 mnc=2 vodaddr=http://vhotwsh.video.qq.com/flv/194/76/a0010P6OOWO.p202.1.mp4?vkey=1C76BC3A712972C1A9AB6190907AEACA360ADBC3DA63323E6F0AD7A71B1C284E638B1BC59A258185&sha=&level=3&br=200&fmt=hd&sdtfrom=v3000&platform=10403 app=2.2.1.3326 block_detail=no_play_block_info os_version=6.1.3 resolution=640*960 get_format_time=-1 get_url_time=-1 video_time=2170 dev_model=iPhone use_dlna=-1 play_source=1 net_ok=-65535 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=fb71a0edda8b1030b355d48564437054 dray_release=-1 market_id=1 pay_status=-1 pay_status_ios=-2 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=9zp9bafa1i28mfd mac=no_mac log_flag=51";
//		messageQueue.offer(new HinaEvent(log.getBytes(), "tptsvr.exe_51.log.mobile"));
//		log = "2013-07-02 14:15:00.386 [22446] [info] remote=221.207.84.107 qq=0 devid=45F51CDC-8468-4B56-986B-1E77C1664DC4 devtype=4 stick=1372737895722 ctick=1372745690219 ptick=905 cdnid=-1 vodtype=2 playno=-1 playid=u0012peega8 vodf=2 block=0 blocktime=0 blockb=-1 blocke=-1 seek=0 seektime=0 seekb=0 seeke=0 play=6 errcode=0 nettype=1 netstrength=-1 mcc=460 mnc=0 vodaddr=file://localhost/var/mobile/Applications/3D540843-B9D0-410F-91CD-6CC78FBFCFD1/Documents/Caches/Media/1c1nnxpt6tb0c9s/u0012peega8.mp4 app=2.2.1.3326 block_detail=no_play_block_info os_version=6.1.3 resolution=640*960 get_format_time=-1 get_url_time=-1 video_time=2722 dev_model=iPhone use_dlna=-1 play_source=2 net_ok=1 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=92889d389ec71030b355d48564437054 dray_release=-1 market_id=1 pay_status=-1 pay_status_ios=-2 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=1c1nnxpt6tb0c9s mac=no_mac log_flag=51";
//		messageQueue.offer(new HinaEvent(log.getBytes(), "tptsvr.exe_51.log.mobile"));
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
		if (log == null 
				|| System.currentTimeMillis() - log.getTimestamp() > 30 * 60 * 1000 ) {
			statics.timeoutPacket.getAndIncrement();
			return;
		}
		
		for (Map.Entry<Integer, ItilRule> entry : fr.getWarningRules().entrySet()) {
			if (entry.getValue().validate(log)) {
				List<Object> tuple = new ArrayList<Object>();
				// first item is category
				tuple.add(log.getCategory());
				// second item is itil id
				tuple.add(entry.getKey());
				// third item is timestamp
				tuple.add(log.getTimestamp());
				
				List<String> body = new ArrayList<String>();
				for (Map.Entry<String, String> field : log.getFields().entrySet()) {
					body.add(new String(field.getKey() + "=" + field.getValue()));
				}
				
				tuple.add(Utils.join(body, "\t"));
				
				collector.emit(StreamingTopology.STREAM_ID, tuple);
//				statics.emitStream.getAndIncrement();	
				statics.getStatics(event.getCategory()).emitStream.getAndIncrement();
			} else {
				statics.getStatics(event.getCategory()).noValidStream.getAndIncrement();
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
			logger.warn("failed to parse "+ event.getCategory() +" timestamp: "
					+ Utils.stringifyException(e));
			return null;
		}
		entry.setTimestamp(date.getTime());
		
		for (int i = 2; i < items.length; i++) {
			String[] keyVal = items[i].split("\\s*=\\s*", 2);
			if (keyVal.length != 2
					|| !exprs.contains(keyVal[0]))
				continue;
			
			entry.addField(keyVal[0], keyVal[1]);
		}
		
		return entry;
	}
	
	public static void main(String[] args) {
		String body = "2013-07-02 14:15:00.002 [22446] [info] remote=171.123.52.86 qq=0 devid=B16A6B29-129B-4FB2-9379-5C1FB6898966 devtype=3 stick=1372679865974 ctick=1372745505662 ptick=512 cdnid=203 vodtype=3 playno=-1 playid=T00103WJHNb vodf=2 block=0 blocktime=0 blockb=-1 blocke=-1 seek=0 seektime=0 seekb=0 seeke=0 play=194 errcode=0 nettype=1 netstrength=-1 mcc=0 mnc=0 vodaddr=http://221.204.198.24/vlive.qqvideo.tc.qq.com/T00103WJHNb.p202.1.mp4?vkey=CDDA1D9FDEAC5E6EB231A829A486D2FE493E8D81908ADAFF025017BF26F5C5C8AB535A15B18CEBD7&sha=&level=3&br=200&fmt=hd&sdtfrom=v4000&platform=10103 app=2.2.1.2512 block_detail=no_play_block_info os_version=6.1.2 resolution=1536*2048 get_format_time=-1 get_url_time=-1 video_time=193 dev_model=iPad use_dlna=-1 play_source=1 net_ok=1 def_switch=-1 blocktime_1=0 def_switch_seq=no_def_switch_seq downed_data_size=-1 guid=6a737968e7641030b355d48564437054 dray_release=-1 market_id=1 pay_status=-1 pay_status_ios=-2 skip=-1 pre_load=-1 sd_card_flag=-1 imei=no_imei album=hmc0z7kb4pldg4d mac=60:FE:C5:75:77:AE log_flag=51";
		Set<String> exprs = new HashSet<String>();
		exprs.add("app");
		exprs.add("remote");
		exprs.add("vodaddr");
		exprs.add("ptick");
		exprs.add("errcode");
		
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
			return;
		}
		System.out.println(date.getTime());
		
		for (int i = 2; i < items.length; i++) {
			String[] keyVal = items[i].split("\\s*=\\s*", 2);
			if (keyVal.length != 2
					|| !exprs.contains(keyVal[0]))
				continue;
			
			System.out.println("######" + keyVal[0] + ":" + keyVal[1]);
		}
	}
}
