package com.tencent.qqlive.streaming.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.qqlive.streaming.util.Utils;

public class WarningConfigDao {
	private static final Logger logger = LoggerFactory.getLogger(WarningConfigDao.class);
	
	private Connection conn = null;
	
	public WarningConfigDao(Connection conn) {
		this.conn = conn;
	}
	
	public List<String> getAllStatsFiles() throws SQLException {
		Statement statement = conn.createStatement();
//		statement.executeUpdate("set names gbk");
		
		String sql = "select distinct f_log_file_name from d_real_time_statis_conf.t_cmd";
		logger.info("execute Query: " + sql);
		
		ResultSet rs = statement.executeQuery(sql);
		List<String> retList = new ArrayList<String>();
		
		while(rs.next()) {
			retList.add(rs.getString(1));
		}
		
		return retList;
	}
	
	public FileRule getRuleForFile(String file) throws SQLException {
		FileRule result = new FileRule();
		
		result.setWarningRules(getItemRuleForFile(file));
		result.setSegmentRules(getSegmentRuleForFile(file));
		result.getExprs(); // eagerly init
		
		
		return result;
	}
	
	public Map<Integer, ItilRule> getItemRuleForFile(String file) throws SQLException {
		Statement statement = conn.createStatement();
//		statement.executeUpdate("set names gbk");
		
		String sql = "select * from d_real_time_statis_conf.t_real_data_warn_config " +
				"where f_log_id in (select f_log_id from d_real_time_statis_conf.t_cmd where f_log_file_name = \"" + file + "\")";
		logger.info("execute Query: " + sql);
		
		ResultSet rs = statement.executeQuery(sql);
		
		Map<Integer, ItilRule> result = new HashMap<Integer, ItilRule>();
		while(rs.next()) {
			try {
				ItilRule wr = new ItilRule();
				
				int itilID = rs.getInt(1);
				wr.setItilID(itilID);
				
				String itemName = rs.getString(2);				
				ElementaryArithmetic arithExpr = new ElementaryArithmetic(itemName);
				wr.setArithExpr(arithExpr);
				
				double motion = rs.getDouble(3);
				wr.setMotion(motion);
				
				Map<String, ItemRange> itemRanges = ItemRange.valueOf(rs.getString(4));
				wr.setItemRanges(itemRanges);
				
				int compareType = rs.getInt(5);
				wr.setCompareType(compareType);
				
				int zoomSize = rs.getInt(6);
				wr.setZoomSize(zoomSize);
				
				String itilDesc = rs.getString(7);
				wr.setItilDesc(itilDesc);
				
				String business = rs.getString(8);
				wr.setBusiness(business);
				
				String receiver = rs.getString(9);
				wr.setReceiver(receiver);
				
				List<FieldRelation> fieldRelations = FieldRelation.valueOf(rs.getString(10));
				wr.setFieldRelations(fieldRelations);
				
				HourMotion hourMotion = HourMotion.valueOf(rs.getString(12));
				wr.setHourMotion(hourMotion);
				
				int msgWarnPeriod = rs.getInt(14);
				wr.setMsgWarnPeriod(msgWarnPeriod);
				
				int maxWarnCount = rs.getInt(15);
				wr.setMaxWarnCount(maxWarnCount);
				
				result.put(itilID, wr);
			} catch (Exception e) {
				logger.error("failed to parse: " + Utils.stringifyException(e));
			}
		}
		
		return result;
	}
	
	public Map<String, SegmentRule> getSegmentRuleForFile(String file) throws SQLException {
		Statement statement = conn.createStatement();
//		statement.executeUpdate("set names gbk");
		
		String sql = "select * from d_real_time_statis_conf.t_real_data_warn_segment_config " +
				"where f_log_id in (select f_log_id from d_real_time_statis_conf.t_cmd where f_log_file_name = \"" + file + "\")";
		logger.info("execute Query: " + sql);
		
		ResultSet rs = statement.executeQuery(sql);
		
		Map<String, SegmentRule> result = new HashMap<String, SegmentRule>();
		while(rs.next()) {
			try {
				SegmentRule segRule = new SegmentRule();
				
//				System.out.println("------" + rs.getString(2));
				List<SegmentRule.Segment> rules = SegmentRule.Segment.valueof(rs.getString(2));
				segRule.setRules(rules);
				
				double contribMinRate = rs.getDouble(3);
				segRule.setContribMinRate(contribMinRate);
				
				result.put(segRule.getDimension(null).getDimension(), segRule);
			} catch (Exception e) {
				logger.error("failed to parse: " + Utils.stringifyException(e));
			}
		}
		
		return result;
	}
}
