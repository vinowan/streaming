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
		
		String sql = "select distinct f_log_file_name from d_auto_statistic.t_cmd";
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
		result.setLatitudeRules(getLatitudeRuleForFile(file));
		result.getExprs(); // eagerly init
		
		return result;
	}
	
	public Map<Integer, ItemRule> getItemRuleForFile(String file) throws SQLException {
		Statement statement = conn.createStatement();
//		statement.executeUpdate("set names gbk");
		
		String sql = "select * from d_auto_statistic.t_real_data_warn_config " +
				"where f_log_id in (select f_log_id from d_auto_statistic.t_cmd where f_log_file_name = \"" + file + "\")";
		logger.info("execute Query: " + sql);
		
		ResultSet rs = statement.executeQuery(sql);
		
		Map<Integer, ItemRule> result = new HashMap<Integer, ItemRule>();
		while(rs.next()) {
			try {
				ItemRule wr = new ItemRule();
				
				int itilID = rs.getInt(1);
				wr.setItilID(itilID);
				
				String itemName = rs.getString(2);
				wr.setItemName(itemName);
				
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
				
				HourMotion hourMotion = HourMotion.valueOf(rs.getString(11));
				wr.setHourMotion(hourMotion);
				
				int msgWarnPeriod = rs.getInt(13);
				wr.setMsgWarnPeriod(msgWarnPeriod);
				
				int maxWarnCount = rs.getInt(14);
				wr.setMaxWarnCount(maxWarnCount);
				
				result.put(itilID, wr);
			} catch (Exception e) {
				logger.error("failed to parse: " + Utils.stringifyException(e));
			}
		}
		
		return result;
	}
	
	public List<LatitudeRule> getLatitudeRuleForFile(String file) throws SQLException {
		Statement statement = conn.createStatement();
//		statement.executeUpdate("set names gbk");
		
		String sql = "select * from d_auto_statistic.t_real_data_analysis_latitude_config " +
				"where f_log_id in (select f_log_id from d_auto_statistic.t_cmd where f_log_file_name = \"" + file + "\")";
		logger.info("execute Query: " + sql);
		
		ResultSet rs = statement.executeQuery(sql);
		
		List<LatitudeRule> result = new ArrayList<LatitudeRule>();
		while(rs.next()) {
			LatitudeRule lr = new LatitudeRule();
			
			String itemName = rs.getString(2);
			lr.setItemName(itemName);
			
			int operation = rs.getInt(3);
			lr.setOperation(operation);
			
			String category = rs.getString(4);
			lr.setCategory(category);
			
			double contribMinRate = rs.getDouble(5);
			lr.setContribMinRate(contribMinRate);
			
			result.add(lr);
		}
		
		return result;
	}
}
