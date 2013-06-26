package com.tencent.qqlive.streaming.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarningDataDao {
	private static final Logger logger = LoggerFactory.getLogger(WarningConfigDao.class);
	
	private Connection conn = null;

	public WarningDataDao(Connection conn) {
		this.conn = conn;
	}
	
	public void insertItilMonitor(long timestamp, int itilID, double result) throws SQLException {
		timestamp -= timestamp % (300 * 1000);
		
		Date date = new Date(timestamp);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		
		String tsStr = formatter.format(date);
		String tableName = "t_monitor_" + tsStr;
		
		Statement statement = conn.createStatement();
		String sql = String.format("insert into d_data_manager.%s values(from_unixtime(%d), %d, %.2f)", tableName, timestamp/1000, itilID, result);
		logger.debug("execute sql: " + sql);
		System.out.println(sql);
		
		statement.executeUpdate(sql);
	}
	
	public void insertSMSWarning(long timestamp, int itilID, String itilDesc, String bussiness, double result, String range) throws SQLException {
		timestamp -= timestamp % (300 * 1000);
		
		Date date = new Date(timestamp);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		String tsStr = formatter.format(date);
		
		Statement statement = conn.createStatement();
		String sql = String.format("insert into d_data_manager.t_real_data_sms_warn(f_time, f_itil_id, f_itil_desc, f_business, f_value, f_range) " +
				"values('%s', %d, '%s', '%s', %.2f, '%s')", tsStr, itilID, itilDesc, bussiness, result, range);
		logger.debug("execute sql: " + sql);
		System.out.println(sql);
		
		statement.executeUpdate(sql);
	}
	
	public void insertEMailWarning(long timestamp, int itilID, String itilDesc, String categoryDesc, String categoryVal, 
			double categoryRes, double totoalResult, String range, double contribution) throws SQLException {
		timestamp -= timestamp % (300 * 1000);
		
		Date date = new Date(timestamp);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		String tsStr = formatter.format(date);
		
		Statement statement = conn.createStatement();
		String sql = String.format("insert into d_data_manager.t_real_data_email_warn(f_time, f_itil_id, f_itil_desc, f_category_desc, f_category_value, f_category_result, f_total_result, f_range, f_contribution) " +
				"values('%s', %d, '%s', '%s', '%s', %.2f, %.2f, '%s', %.2f)", 
				tsStr, itilID, itilDesc, categoryDesc, categoryVal, categoryRes, totoalResult, range, contribution);
		logger.debug("execute sql: " + sql);
		
		statement.executeUpdate(sql);
	}
	
	//是否需要发送告警信息或者告警恢复星系
	//分三种情况
	//1. 告警
	//    a.有未恢复告警，那么不告警
	//    b.没有未恢复告警，那么告警
	//2. 恢复
	//    a. 有未恢复告警，发恢复短信
	//    b. 没有位恢复告警，不发送
	//Status，true --告警，false--恢复
	//返回值：//0 ---不告警, >0 --告警, <0 --- 恢复
	public int isSendWarn(int itilID, boolean status) throws SQLException {
		int result = -1;
		
		Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			    ResultSet.CONCUR_READ_ONLY);
		String sql = null;
		if (status) {
			sql = String.format("select f_warn_start_time " +
					"from d_data_manager.t_monitor_warn_recovery " +
					"where f_itil_id = %d and  f_warn_start_time <> '' " +
					"and f_warn_stop_time is null " +
					"order by f_warn_start_time desc limit 1", itilID);
			
			logger.debug("execute sql: " + sql);
			
			ResultSet rs = statement.executeQuery(sql);
			if (resultSetSize(rs) > 0) {
				result = 0;
			} else {
				result = 1;
				
				sql = String.format("insert into d_data_manager.t_monitor_warn_recovery(f_itil_id, f_warn_start_time) values(%d, now())", itilID);
				
				logger.debug("execute sql: " + sql);
				
				statement.executeUpdate(sql);				
			}
		} else {
			sql = String.format("select f_warn_start_time " +
					"from d_data_manager.t_monitor_warn_recovery " +
					"where f_itil_id = %d and  f_warn_start_time <> '' " +
					"and f_warn_stop_time is null " +
					"order by f_warn_start_time desc limit 1"
					, itilID);
			
			logger.debug("execute sql: " + sql);
			
			ResultSet rs = statement.executeQuery(sql);
			if (resultSetSize(rs) == 0) {
				result = 0;
			} else {
				result = -1;
				
				sql = String.format("update d_data_manager.t_monitor_warn_recovery set f_warn_stop_time  = now() " +
						"where f_itil_id = %d and f_warn_stop_time is null", itilID);
				
				logger.debug("execute sql: " + sql);
				
				statement.executeUpdate(sql);
			}
		}
		
		return result;
	}
	
	private int resultSetSize(ResultSet rs) throws SQLException {
		if (!rs.last())
			return 0;
		
		int size = rs.getRow();
		rs.beforeFirst();
		
		return size;
	}
}
