package com.neu.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SqlHelper {

	private DBUtil dbUtil = DBUtil.getInstance();
	
	private Connection conn = null;
	
	public SqlHelper(){
		try {
			conn = dbUtil.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据指定SQL指定参数查询
	 * 
	 * @param sql 查询语句
	 * @param parms 多个参数
	 * @return 查询结果
	 * @throws SQLException 
	 */
	public List<Map<String, Object>> executeQuery(String sql, Object ... parms) throws SQLException{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Map<String, Object>> lists = new ArrayList<Map<String, Object>>();
		try {
			pstmt = conn.prepareStatement(sql);
			// 占位符索引。（'?'的索引）
			int placeHolderIndex = 1;
			for(int i = 0; i < parms.length; i++) {
				if(parms[i] instanceof String) {
					String params = (String) parms[i];
					String[] paramArr = params.split(",");
					for(int j = 0; j < paramArr.length; j++) {
						pstmt.setObject(placeHolderIndex, paramArr[j].trim());
						placeHolderIndex++;
					}
				}else {
					pstmt.setObject(placeHolderIndex, parms[i]);
					placeHolderIndex++;
				}
				
			}
			rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (null != rs && rs.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 0; i < columnCount; i++) {
                    String name = rsmd.getColumnName(i + 1);
                    Object value = rs.getObject(name);
                    map.put(name, value);
                }
                lists.add(map);
            }
		} catch (SQLException e) {
			throw e;
		}finally {
			try {
				this.close(pstmt, rs);
			} catch (SQLException e) {
				throw e;
			}
		}
		
		return lists;
	}
	
	/**
	 * 批量执行指定点SQL语句
	 * 
	 * @param sql sql语句
	 * @param parms 多个参数
	 * @throws SQLException 
	 */
	public int[] executeBatch(String sql, List<List<String>> params) throws SQLException{
		PreparedStatement pstmt = null;
		int[] result = null;
		conn.setAutoCommit(false);
		pstmt = conn.prepareStatement(sql);
		for(int i = 0; i < params.size(); i++) {
			List<String> rowParam = params.get(i);
			for (int j = 0; j < rowParam.size(); j++) {
				pstmt.setObject(j+1, rowParam.get(j));
			}
			pstmt.addBatch();
		}
		result = pstmt.executeBatch();
		conn.commit();
		conn.setAutoCommit(true);
		return result;
	}
	
	/**
	 * 执行指定点SQL语句
	 * 
	 * @param sql sql语句
	 * @param parms 多个参数
	 * @throws SQLException 
	 */
	public int executeUpdate(String sql, Object ...parms) throws SQLException{
		PreparedStatement pstmt = null;
		
		pstmt = conn.prepareStatement(sql);
		// 占位符索引。（'?'的索引）
		int placeHolderIndex = 1;
		for(int i = 0; i < parms.length; i++) {
			if(parms[i] instanceof String) {
				String params = (String) parms[i];
				String[] paramArr = params.split(",");
				for(int j = 0; j < paramArr.length; j++) {
					pstmt.setObject(placeHolderIndex, paramArr[j].trim());
					placeHolderIndex++;
				}
			}else if (parms[i] instanceof StringBuffer) {
				pstmt.setObject(placeHolderIndex, parms[i].toString());
				placeHolderIndex++;
			}else {
				pstmt.setObject(placeHolderIndex, parms[i]);
				placeHolderIndex++;
			}
		}
		int result = pstmt.executeUpdate();
		pstmt.close();
		return result;
	}
	
	
	public void close(Statement stmt, ResultSet rs) throws SQLException{
		if (null != rs)
            rs.close();
        if (null != stmt)
        	stmt.close();
	}
	
	public void setAutoCommit(boolean flag) throws SQLException {
		this.conn.setAutoCommit(flag);
	}
	
	public void commit() throws SQLException{
		this.conn.commit();
	}
	
	public void rollBack() throws SQLException{
		this.conn.rollback();
	}
	
	public void closeConnection(){
		dbUtil.closeConnection(conn);
	}
	
	/**
	 * 生成多个?。用于生成带In的sql语句。
	 * @param length
	 * @return
	 */
	public String preparePlaceHolders(int length) {

	    StringBuilder builder = new StringBuilder();

	    for (int i = 0; i < length;) {

	        builder.append("?");

	        if (++i < length) {

	            builder.append(",");

	        }

	    }
	    return builder.toString();
	}
	
	public static void main(String[] args) throws SQLException {
		SqlHelper sh = new SqlHelper();
//		List<Map<String, Object>> result = sh.executeQuery("SELECT * FROM ALTITUDE WHERE ID=?", 2);
//		for (Map<String, Object> map : result) {
//			System.out.println(map.get("WIND_ID"));
//		}
		List<List<String>> params = new ArrayList<List<String>>();
		List<String> rowParam = new ArrayList<String>();
		rowParam.add("1");
		rowParam.add("2");
		rowParam.add("3");
		params.add(rowParam);
		sh.executeBatch("insert into ENERGYDAY(province,farm_name,factory) values(?,?,?)", params);
		sh.closeConnection();
	}
}
