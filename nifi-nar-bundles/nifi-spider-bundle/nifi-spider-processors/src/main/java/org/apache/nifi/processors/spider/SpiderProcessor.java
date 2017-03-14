package org.apache.nifi.processors.spider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.json.JSONException;
import org.json.JSONObject;

@SideEffectFree
@Tags({ "爬虫", "新闻" })
@CapabilityDescription("通过爬虫获取指定新闻关键字的新闻条数，将结果保存到mysql数据库中.")
public class SpiderProcessor extends AbstractProcessor {

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private String rslt = null;
	private JSONObject objParam = null;

	public static final PropertyDescriptor FILE_KEYWORD = new PropertyDescriptor.Builder().name("File Keyword")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("Spider Success")
			.build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(FILE_KEYWORD);

		this.properties = Collections.unmodifiableList(properties);

		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final ComponentLog log = this.getLogger();
		final AtomicReference<String> value = new AtomicReference<>();
		String results = null;
		FlowFile flowfile = session.get();
		if (null == flowfile) {
			flowfile = session.create();
		}
		log.info("读取的文件名：" + flowfile.getAttribute("filename"));
		Map<PropertyDescriptor, String> props = context.getProperties();
		String file_name = flowfile.getAttribute("filename");
		String file_keyword = props.get(FILE_KEYWORD);
		log.info("爬虫关键字：" + file_keyword);
		// spider
		long newsNum = keywordSpider(file_keyword);
//		long newsNum = 88474;
		log.info("爬虫新闻数量：" + newsNum);
		if (newsNum == -1) {
			rslt = "Spider Faild";
			getLogger().info("爬虫任务失败！");
		} else {
			objParam = new JSONObject();
			// spider data
			try {
				objParam.put("type", 1);
				// filename,keyword,newsNum
				objParam.put("results", file_name+","+ file_keyword+"," + newsNum);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			rslt = objParam.toString();
		}
		value.set(rslt);
		results = value.get();
		if (results != null) {
			flowfile = session.write(flowfile, new StreamCallback() {
				public void process(InputStream in, OutputStream out) throws IOException {
					OutputStreamWriter oss = new OutputStreamWriter(out, "UTF-8");
					oss.write(rslt);
					oss.flush();
					oss.close();
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

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException {

	}
}
