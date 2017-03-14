package org.apache.nifi.processors.spider;

import org.json.JSONException;

public class StatementUtil {

	private final String StringLen = "(255)";
	private final String IntegerLen = "(11)";
	private final String DoubleLen = "(255,5)";

	public String createTable(String tableName, String[] argsTypes, String[] argsValues) {
		StringBuilder args = new StringBuilder();
		if (argsTypes.length != argsValues.length) {
			return null;
		} else {
			for (int i = 0; i < argsTypes.length; i++) {
				if (i == 0) {
//					args.append("DROP TABLE IF EXISTS ").append(tableName).append(";")
					args.append("CREATE TABLE ").append(tableName).append("(").append(argsValues[i]).append(" ")
					.append(argsTypes[i]).append(" primary key not null auto_increment ,");
				} else {
					args.append(argsValues[i]).append(" ").append(argsTypes[i]);
					if (argsTypes[i].equals("int")) {
						args.append(IntegerLen);
					} else if (argsTypes[i].equals("varchar")) {
						args.append(StringLen);
					} else if(argsTypes[i].equals("double")){
						args.append(DoubleLen);
					} else {
					}
					if (i != argsValues.length - 1) {
						args.append(",");
					}
				}
			}
			args.append(");");
			return args.toString();
		}
	}

	public String selectSql(String tableName,String fileName){
		StringBuilder args = new StringBuilder();
		args.append("SELECT fileNumber FROM ").append(tableName).append(" where ").append("fileName =").append("'").append(fileName).append("'");
		return args.toString();
	}
	
	public String insertSqlByType(String tableName, String[] argsTypes, String[] argsValues, int type,
			String[] results) throws JSONException {
		StringBuilder args = new StringBuilder();
		int startFlag = 1;
		int endFlag = 4;
		if (type == 1) {
			System.out.println("type：" + type + "的数据：" + results);
			startFlag = 1;
			endFlag = 4;
		} else if(type == 99){
			System.out.println("type：" + type + "的数据：" + results);
			startFlag = 1;
			endFlag = 9;
		}else {
			return null;
		}

		for (int i = startFlag; i < endFlag; i++) {
			if (i == startFlag) {
				args.append("INSERT INTO ").append(tableName).append("(").append(argsValues[i]).append(",");
			} else {
				args.append(argsValues[i]);
				if (i != endFlag - 1) {
					args.append(",");
				}

			}
		}
		args.append(") values (");

		for (int i = startFlag; i < endFlag; i++) {
			if (argsTypes[i].equals("int")||argsTypes[i].equals("double")) {
				args.append(results[i]);
			} else if (argsTypes[i].equals("varchar")||argsTypes[i].equals("longtext")) {
				args.append("'").append(results[i]).append("'");
			} else {
				break;
			}

			if (i != endFlag - 1) {
				args.append(",");
			}
		}
		args.append(");");
		return args.toString();
	}

	public String updateSqlByType(String tableName, String[] argsTypes, String[] argsValues, int type,
			String[] results) throws JSONException {
		StringBuilder args = new StringBuilder();
		int startFlag = 0;
		int endFlag = 0;
		String fileName = results[0];

		if (type == 3) {
			System.out.println("type：" + type + "的数据：" + results);
			startFlag = 8;
			endFlag = 9;

		} else if (type == 2) {
			System.out.println("type：" + type + "的数据：" + results);
			startFlag = 4;
			endFlag = 8;

		} else {
			return null;
		}

		int j = 1;
		for (int i = startFlag; i < endFlag; i++) {
			if (i == startFlag) {
				args.append("UPDATE ").append(tableName).append(" set ").append(argsValues[i]).append("=");

			} else {
				args.append(",").append(argsValues[i]).append("=");
			}

			if (argsTypes[i].equals("int")||argsTypes[i].equals("double")) {
				args.append(results[j]);
			} else if (argsTypes[i].equals("varchar")||argsTypes[i].equals("longtext")) {
				args.append("'").append(results[j]).append("'");
			} else {
				break;
			}
			j++;
		}
		args.append(" where ").append(argsValues[1]).append("= '").append(fileName).append("'");
		return args.toString();
	}

	public String isExistTb(String tableName){
		StringBuilder args = new StringBuilder();
		args.append("DROP TABLE IF EXISTS ").append(tableName).append(";");
		return args.toString();
	}
	
	public String[] splitArgs(String args) {
		return args.split(",");
	}

	public static void main(String args[]) throws JSONException {
		String tableName = "test001";
		String argsType = "int,varchar,varchar,int,double,double,double,double,longtext";
		String argsValue = "id,fileName,keyword,newsNum,year_total_income,last_year_total_income,year_total_cost,last_year_total_cost,peopleName";

//		 spider sql
//		 String values = "-1,6650,江苏吴中实业,2988";
//		 StatementUtil st = new StatementUtil();
//		 String ct = st.createTable(tableName, st.splitArgs(argsType),
//		 st.splitArgs(argsValue));
//		 String it = st.insertSqlByType(tableName, st.splitArgs(argsType),
//		 st.splitArgs(argsValue),1, st.splitArgs(values));
//		 System.out.println("建表语句：" + ct);
//		 System.out.println("插入语句：" + it);

		// peopleName sql
//		String values = "6650,王大头|路大头|吕大头|梁大头";
//		StatementUtil st = new StatementUtil();
//		String ct = st.createTable(tableName, st.splitArgs(argsType), st.splitArgs(argsValue));
//		String ut = st.updateSqlByType(tableName, st.splitArgs(argsType), st.splitArgs(argsValue), 3,
//				st.splitArgs(values));
//		System.out.println("建表语句：" + ct);
//		System.out.println("更新语句：" + ut);
		
		// financial sql
//		String values = "6,3875301454.29,3722974684.57,3742447274.62,3683662321.97";
//		StatementUtil st = new StatementUtil();
//		String ct = st.createTable(tableName, st.splitArgs(argsType), st.splitArgs(argsValue));
//		String ut = st.updateSqlByType(tableName, st.splitArgs(argsType), st.splitArgs(argsValue), 2,
//				st.splitArgs(values));
//		System.out.println("建表语句：" + ct);
//		System.out.println("更新语句：" + ut);
		 
//		combine sql
		 String values = "-1,江苏吴中实业股份有限公司2013年度报告.txt,江苏吴中实业股份有限公司,1080,3875301454.29,3722974684.57,3742447274.62,3683662321.97,赵唯一|姚建林|许良枝";
		 StatementUtil st = new StatementUtil();
		 String ct = st.createTable(tableName, st.splitArgs(argsType),
		 st.splitArgs(argsValue));
		 String it = st.insertSqlByType(tableName, st.splitArgs(argsType),
		 st.splitArgs(argsValue),99, st.splitArgs(values));
		 System.out.println("建表语句：" + ct);
		 System.out.println("插入语句：" + it);
		 
	}
}
