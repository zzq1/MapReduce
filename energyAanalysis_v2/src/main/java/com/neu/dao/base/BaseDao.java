package com.neu.dao.base;

import java.sql.SQLException;

import com.neu.utils.SqlHelper;

public class BaseDao {

	protected SqlHelper sqlHelper = null;
	
	public BaseDao(){
		sqlHelper = new SqlHelper();
	}
	
	/**
	 * 开启事物
	 * @throws SQLException
	 */
	public void startTransanction() throws SQLException{
		sqlHelper.setAutoCommit(false);
	}
	
	/**
	 * 结束事物
	 * @throws SQLException
	 */
	public void endTransanction() throws SQLException{
		sqlHelper.commit();
		sqlHelper.setAutoCommit(true);
	}
	
	/**
	 * 关闭连接
	 */
	public void close(){
		sqlHelper.closeConnection();
	}
	
	/**
	 * 回滚事物
	 * @throws SQLException
	 */
	public void rollBack() throws SQLException{
		sqlHelper.rollBack();
		
	}
	
}
