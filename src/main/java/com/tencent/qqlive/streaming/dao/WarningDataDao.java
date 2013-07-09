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
		String sql = String.format("insert into d_real_time_statis_data.%s values(from_unixtime(%d), %.2f, %d)", tableName, timestamp/1000, result, itilID);
		logger.debug("execute sql: " + sql);
		System.out.println(sql);
		
		statement.executeUpdate(sql);
	}
	
	public void insertSMSWarning(long timestamp, int itilID, String itilDesc, String bussiness, 
			String recoveryDesc, double result, String range, String receiver) throws SQLException {
		timestamp -= timestamp % (300 * 1000);
		
		Date date = new Date(timestamp);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		String tsStr = formatter.format(date);
		
		Statement statement = conn.createStatement();
		String sql = String.format("insert into d_real_time_statis_data.t_real_data_sms_warn(f_time, f_itil_id, f_itil_desc, f_business, f_recovery_desc, f_value, f_range, f_msg_recver) " +
				"values('%s', %d, '%s', '%s', '%s', %.2f, '%s', '%s')", tsStr, itilID, itilDesc, bussiness, recoveryDesc, result, range, receiver);
		logger.debug("execute sql: " + sql);
		System.out.println(sql);
		
		statement.executeUpdate(sql);
	}
	
	public void insertEMailWarning(long timestamp, int itilID, String itilDesc, String categoryDesc, String categoryVal, 
			double categoryRes, double totoalResult, String range, double contribution, String receiver) throws SQLException {
		timestamp -= timestamp % (300 * 1000);
		
		Date date = new Date(timestamp);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		String tsStr = formatter.format(date);
		
		Statement statement = conn.createStatement();
		String sql = String.format("insert into d_real_time_statis_data.t_real_data_email_warn(f_time, f_itil_id, f_itil_desc, f_category_desc, f_category_value, f_category_result, f_total_result, f_range, f_contribution, f_msg_recver) " +
				"values('%s', %d, '%s', '%s', '%s', %.2f, %.2f, '%s', %.2f, '%s')", 
				tsStr, itilID, itilDesc, categoryDesc, categoryVal, categoryRes, totoalResult, range, contribution, receiver);
		logger.debug("execute sql: " + sql);
		System.out.println(sql);
		
		statement.executeUpdate(sql);
	}
	
	//�Ƿ���Ҫ���͸澯��Ϣ���߸澯�ָ���ϵ
	//���������
	//1. �澯
	//    a.��δ�ָ��澯����ô���澯
	//    b.û��δ�ָ��澯����ô�澯
	//2. �ָ�
	//    a. ��δ�ָ��澯�����ָ�����
	//    b. û��λ�ָ��澯��������
	//Status��true --�澯��false--�ָ�
	//����ֵ��//0 ---���澯, >0 --�澯, <0 --- �ָ�
	public int isSendWarn(int itilID, boolean status) throws SQLException {
		int result = -1;
		
		Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			    ResultSet.CONCUR_READ_ONLY);
		String sql = null;
		if (status) {
			sql = String.format("select f_warn_start_time " +
					"from d_real_time_statis_data.t_monitor_warn_recovery " +
					"where f_itil_id = %d and  f_warn_start_time <> '' " +
					"and f_warn_stop_time is null " +
					"order by f_warn_start_time desc limit 1", itilID);
			
			logger.debug("execute sql: " + sql);
			
			ResultSet rs = statement.executeQuery(sql);
			if (resultSetSize(rs) > 0) {
				result = 0;
			} else {
				result = 1;
				
				sql = String.format("insert into d_real_time_statis_data.t_monitor_warn_recovery(f_itil_id, f_warn_start_time) values(%d, now())", itilID);
				
				logger.debug("execute sql: " + sql);
				
				statement.executeUpdate(sql);				
			}
		} else {
			sql = String.format("select f_warn_start_time " +
					"from d_real_time_statis_data.t_monitor_warn_recovery " +
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
				
				sql = String.format("update d_real_time_statis_data.t_monitor_warn_recovery set f_warn_stop_time  = now() " +
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
