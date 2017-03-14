package org.apache.nifi.processors.spider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MysqlUtil {
	private Connection _conn;
	private Statement _stmt;
	protected static final int OPERATOR_LARGER = 1;
	protected static final int OPERATOR_SMALLER = 2;
	private static final String MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=UTF-8";
	private static final String MYSQL_USER = "root";
	private static final String MYSQL_PWD = "root";

	public Connection getConnInstance(String mysql_connecturl, String mysql_username, String mysql_password) throws SQLException {
		if (_conn == null) {
			synchronized (Connection.class) {
				if (_conn == null) {
					try {
						Class.forName("com.mysql.jdbc.Driver").newInstance();
					} catch (InstantiationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					_conn = DriverManager.getConnection(mysql_connecturl, mysql_username, mysql_password);
				}
			}
		}
		return _conn;
	}

	public MysqlUtil(String mysql_connecturl, String mysql_username, String mysql_password) {
		try {
			getConnInstance(mysql_connecturl,mysql_username,mysql_password);
		} catch (SQLException ex) {
			Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public ResultSet query(String sql) {
		Object result = null;
		Statement stmt = null;
		ResultSet ret = null;
		try {
			stmt = _conn.createStatement();
			ret = stmt.executeQuery(sql);
		} catch (SQLException ex) {
			Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ex) {
					Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
				}
			}

		}
		return ret;
	}

	public Object edit(String sql) {
		Object result = null;
		Statement stmt = null;
		try {
			stmt = _conn.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException ex) {
			Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ex) {
					Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
				}
			}

		}
		return result;
	}

	protected Object create(String sql) {
		Object result = null;
		Statement stmt = null;
		try {
			stmt = _conn.createStatement();
			stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
//			ResultSet rs = stmt.getGeneratedKeys();
//			rs.next();
//			result = rs.getInt(1);
			return 1;
		} catch (SQLException ex) {
			Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ex) {
					Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
				}
			}

		}
		return result;
	}

	protected Object isExistTb(String sql) {
		Object result = null;
		Statement stmt = null;
		try {
			stmt = _conn.createStatement();
			stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
//			ResultSet rs = stmt.getGeneratedKeys();
//			rs.next();
//			result = rs.getInt(1);
			return 1;
		} catch (SQLException ex) {
			Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ex) {
					Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
				}
			}

		}
		return result;
	}
	
	public void commit() {
		if (this._stmt != null) {
			try {
				this._stmt.executeBatch();
			} catch (SQLException ex) {
				Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
			} finally {
				try {
					this._stmt.close();
					this._stmt = null;
				} catch (SQLException ex) {
					Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
	}

	public void close() {
		try {
			if (this._conn != null) {
				this._conn.close();
			}
		} catch (SQLException ex) {
			Logger.getLogger(MysqlUtil.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public Connection getConn(){
		return _conn;
	}
	public static void main (String args[]){
		MysqlUtil mUtil = new MysqlUtil(MYSQL_URL, MYSQL_USER, MYSQL_PWD);
		System.out.println(mUtil.getConn());
	}
}
