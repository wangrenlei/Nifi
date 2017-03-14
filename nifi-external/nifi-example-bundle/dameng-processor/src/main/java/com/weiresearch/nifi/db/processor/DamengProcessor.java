package com.weiresearch.nifi.db.processor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.ImportTool;

@SideEffectFree
@Tags({ "Dameng", "NIFI ROCKS" })
@CapabilityDescription("Import dameng db data into hdfs.")
public class DamengProcessor extends AbstractProcessor {

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;


	public static final String DRIVER_NAME = "dm.jdbc.driver.DmDriver";

	public static final PropertyDescriptor DAMENG_ADDRESS = new PropertyDescriptor.Builder()
			.name("Dameng Address").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DAMENG_PORT = new PropertyDescriptor.Builder()
			.name("Dameng Port").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DAMENG_DB = new PropertyDescriptor.Builder()
			.name("Dameng DB").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DAMENG_DB_USER = new PropertyDescriptor.Builder()
			.name("Dameng User").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DAMENG_DB_PASSWORD = new PropertyDescriptor.Builder()
			.name("Dameng Password").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DAMENG_DB_TABLE = new PropertyDescriptor.Builder()
			.name("Import Table").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor TARGET_PATH = new PropertyDescriptor.Builder()
			.name("Target Path").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DEFAULT_FS = new PropertyDescriptor.Builder()
			.name("DefaultFS").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MR_HOME = new PropertyDescriptor.Builder()
			.name("MR Home").build();
	
	public static final Relationship SUCCESS = new Relationship.Builder()
			.name("SUCCESS").description("Succes Output").build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(DAMENG_ADDRESS);
		properties.add(DAMENG_PORT);
		properties.add(DAMENG_DB);
		properties.add(DAMENG_DB_USER);
		properties.add(DAMENG_DB_PASSWORD);
		properties.add(DAMENG_DB_TABLE);
		properties.add(TARGET_PATH);
		properties.add(DEFAULT_FS);
		properties.add(MR_HOME);
		
		this.properties = Collections.unmodifiableList(properties);

		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public void onTrigger(final ProcessContext context,
			final ProcessSession session) throws ProcessException {
		final ComponentLog log = this.getLogger();
		final AtomicReference<String> value = new AtomicReference<>();

		FlowFile flowfile = session.get();
		if(null == flowfile){
			flowfile = session.create();
		}
		System.out.println("flowfile is   " + flowfile);
		Map<PropertyDescriptor, String> props = context.getProperties();
		String dm_url = props.get(DAMENG_ADDRESS);
		String dm_port = props.get(DAMENG_PORT);
		String dm_db = props.get(DAMENG_DB);
		String dm_user = props.get(DAMENG_DB_USER);
		String dm_pass = props.get(DAMENG_DB_PASSWORD);
		String dm_table = props.get(DAMENG_DB_TABLE);
		String hdfs_target = props.get(TARGET_PATH);
		String default_fs = props.get(DEFAULT_FS);
		String mr_home = props.get(MR_HOME);
		String rslt = "";// loadDamengData(dm_url,dm_port,dm_db,dm_user,dm_pass,dm_table,hdfs_target);
		try {
			rslt = loadDamengDataSqoop(dm_url, dm_port, dm_db, dm_user,
					dm_pass, dm_table, hdfs_target,default_fs,mr_home);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		value.set(rslt);

		// Write the results to an attribute
		String results = value.get();
		if (results != null) {
			flowfile = session.putAttribute(flowfile, "load_result", results);
		}

		// To write the results back out ot flow file
		flowfile = session.write(flowfile, new OutputStreamCallback() {

			@Override
			public void process(OutputStream out) throws IOException {
				out.write(value.get().getBytes());
			}
		});

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

	public String loadDamengDataSqoop(String dm_url, String dm_port,
			String dm_db, String dm_user, String dm_password, String dm_table,
			String target_path,String default_fs, String mr_home) throws InterruptedException {

		SqoopUtil
				.setUp("dm.jdbc.driver.DmDriver", dm_url, dm_user, dm_password,default_fs,mr_home);
		SqoopUtil.TransferringEntireTableSpecificDir(dm_table, target_path);
		int rslt = SqoopUtil.runIt();
		if(rslt !=0){
			return "导入失败";
		}
		return "导入成功";
	}

	public static void main(String[] args) throws InterruptedException,
			ClassNotFoundException, SQLException {
		DamengProcessor pro = new DamengProcessor();
		// pro.loadDamengData("", "", "", "", "", "", "");
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "");
		String rslt = pro.loadDamengDataSqoop(
				"jdbc:dm://", "", "", "",
				"", "", "hdfs://","","");//
		System.out.println(rslt);

	}
}
