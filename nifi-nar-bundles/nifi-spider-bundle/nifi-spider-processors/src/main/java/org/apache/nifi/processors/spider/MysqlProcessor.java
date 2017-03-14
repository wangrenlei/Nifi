package org.apache.nifi.processors.spider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONException;
import org.json.JSONObject;

@SideEffectFree
@Tags({ "mysql", "创建", "插入", "更新" })
@CapabilityDescription("创建mysql表或者新增，更新mysql信息.")
public class MysqlProcessor extends AbstractProcessor {

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private String line;

	public static final PropertyDescriptor MYSQL_CONNECTURL = new PropertyDescriptor.Builder().name("Mysql ConnectURL")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MYSQL_USERNAME = new PropertyDescriptor.Builder().name("Mysql UserName")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MYSQL_PASSWORD = new PropertyDescriptor.Builder().name("Mysql Password")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MYSQL_TABLENAME = new PropertyDescriptor.Builder().name("Mysql TableName")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MYSQL_ARGSTYPE = new PropertyDescriptor.Builder().name("Update ArgsType")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MYSQL_ARGSVALUE = new PropertyDescriptor.Builder().name("Update ArgsValue")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("Success Insert")
			.build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(MYSQL_CONNECTURL);
		properties.add(MYSQL_USERNAME);
		properties.add(MYSQL_PASSWORD);
		properties.add(MYSQL_TABLENAME);
		properties.add(MYSQL_ARGSTYPE);
		properties.add(MYSQL_ARGSVALUE);

		this.properties = Collections.unmodifiableList(properties);

		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final ComponentLog log = this.getLogger();
		final AtomicReference<String> value = new AtomicReference<>();
		StringBuilder buffer = new StringBuilder();
		String rslt = null;
		String results = null;
		String values = null;
		int type = -1;
		JSONObject dataObj = null;
		FlowFile flowfile = session.get();
		log.info("mysqlProcessor第一次flowfile:" + flowfile);
		if (null == flowfile) {
			flowfile = session.create();
		}

		// 解析读取数据
		session.read(flowfile, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
				BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
				String line = null;
				while ((line = br.readLine()) != null) {
					log.info("========");
					log.info("line :" + line);
					buffer.append(line);
					log.info("========");
				}
				br.close();
				in.close();
			}
		});

		try {
			dataObj = new JSONObject(buffer.toString());
			type = dataObj.getInt("type");
			log.info("type==========" + type);
			values = dataObj.getString("results");
			log.info("values==========" + values);
		} catch (JSONException e1) {
			e1.printStackTrace();
		}

		Map<PropertyDescriptor, String> props = context.getProperties();
		String mysql_connecturl = props.get(MYSQL_CONNECTURL);
		String mysql_username = props.get(MYSQL_USERNAME);
		String mysql_password = props.get(MYSQL_PASSWORD);
		String mysql_tablename = props.get(MYSQL_TABLENAME);
		String mysql_argsType = props.get(MYSQL_ARGSTYPE);
		String mysql_argsvalue = props.get(MYSQL_ARGSVALUE);

		// db option
		try {
			if(type==99){
				rslt = this.loadDataMysql(mysql_connecturl, mysql_username, mysql_password, mysql_tablename, mysql_argsType,
						mysql_argsvalue, type, values);
				log.info("mysql rslt==========" + values);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		value.set(rslt);

		results = value.get();
		if (results != null) {
			flowfile = session.putAttribute(flowfile, "mysql_result", results);
			log.info("第二次flowfile:" + flowfile);
			// To write the results back out ot flow file
			flowfile = session.write(flowfile, new OutputStreamCallback() {
				@Override
				public void process(OutputStream out) throws IOException {
					out.write(value.get().getBytes());
					out.flush();
					out.close();
				}
			});
		}
		session.transfer(flowfile, SUCCESS);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	public long keywordSpider(String keyword) {
		return SpiderUtil.getMention(keyword);
	}

	// mysql_argskey = "int,int,varchar,int" mysql_argsvalue =
	// "id,fileNumber,fileKeyWord,newsNum"
	public String loadDataMysql(String mysql_connecturl, String mysql_username, String mysql_password,
			String mysql_tablename, String mysql_argstype, String mysql_argsvalue, int type, String values)
					throws InterruptedException, JSONException, SQLException {

		// create mysql
		StatementUtil st = new StatementUtil();
		String creaTabSql = null;
		String isExistTabSql = null;
		String fileNumber = "-1";
		MysqlUtil mUtil = new MysqlUtil(mysql_connecturl, mysql_username, mysql_password);
		if (type == 1|| type ==99) {
			creaTabSql = st.createTable(mysql_tablename, st.splitArgs(mysql_argstype), st.splitArgs(mysql_argsvalue));
			this.getLogger().info("creaTabSql=========:" + creaTabSql);
			isExistTabSql = st.isExistTb(mysql_tablename);
			this.getLogger().info("creaTabSql=========:" + isExistTabSql);
		}

		// select mysql
		String fileName = values.split(",")[0];
//		String selectSql = st.selectSql("fileMap", fileName);
//		this.getLogger().info("selectSql=========:" + selectSql);
//		ResultSet rSet = mUtil.query(selectSql);
//		this.getLogger().info("rSet=========:" + rSet);
//		if (rSet != null) {
		if(true){
			// fileNumber = rSet.getInt("fileNumber")+"";
			fileNumber = 6 + "";
			this.getLogger().info("fileNumber=========:" + fileNumber);
			// 替换filename
			String newValues = null;
			if(type == 1){
				newValues = "null," + fileNumber + ",";
			}else{
				newValues = fileNumber + ",";
			}
			String[] valuesStr = st.splitArgs(values);
//			for (int i = 1; i < valuesStr.length; i++) {
//				if (i != valuesStr.length - 1) {
//					newValues += valuesStr[i] + ",";
//				} else {
//					newValues += valuesStr[i];
//				}
//			}
			this.getLogger().info("newValues=========:" + newValues);
			
			if (type == 1|| type == 99) {
				newValues = "null," +values;
				// insert mysql
				this.getLogger().info("newValues2=========:" + newValues);
				String insertSql = st.insertSqlByType(mysql_tablename, st.splitArgs(mysql_argstype),
						st.splitArgs(mysql_argsvalue), type, st.splitArgs(newValues));
				this.getLogger().info("insertSql:" + insertSql);
				mUtil.isExistTb(isExistTabSql);
				Object object = mUtil.create(creaTabSql);
				if (object != null) {
					Object object2 = mUtil.edit(insertSql);
					mUtil.close();
					if (object2 != null) {
						return "插入数据库成功!";
					}
					return "插入数据库是失败!";
				}
				return "建表失败!";
			} else {
				
				newValues = values;
				String updateSql = st.updateSqlByType(mysql_tablename, st.splitArgs(mysql_argstype),
						st.splitArgs(mysql_argsvalue), type, st.splitArgs(newValues));

				this.getLogger().info("updateSql:" + updateSql);
				Object object2 = mUtil.edit(updateSql);
				mUtil.close();
				if (object2 != null) {
					return "更新数据库成功！";
				}
				return "更新数据库失败！";
			}
		}
		return "filemap不存在该文件的映射";
	}

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException {

	}
}
