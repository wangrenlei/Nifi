package org.apache.nifi.processors.spider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
@Tags({ "合并数据流" })
@CapabilityDescription("将各个数据流信息合并为一条数据信息.")
public class CombineDataProcessor extends AbstractProcessor {

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private String rslt = null;
	private JSONObject objParam = null;
	private List<Integer> ls = new ArrayList<Integer>();
	private Map<Integer, String> mp = new HashMap<Integer, String>();

	public static final PropertyDescriptor PROCESSOR_NUM = new PropertyDescriptor.Builder().name("Processor Num")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("Success Combine")
			.build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(PROCESSOR_NUM);

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
		String results = null;
		String values = null;
		int type = -1;
		JSONObject dataObj = null;
		FlowFile flowfile = session.get();
		log.info("combineProcessor第一次flowfile:" + flowfile);
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
		String processor_num = props.get(PROCESSOR_NUM);

		try {
			String combineResult = this.combineData(values, type, Integer.parseInt(processor_num));
			log.info("combineResult结果==========" + combineResult);
			if(combineResult!=null){
				objParam = new JSONObject();
				objParam.put("type", 99);
				objParam.put("results", combineResult);
				rslt = objParam.toString();
				type = 99;
				log.info("combineResult==========" + combineResult);
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
		log.info("rslt++++++++++++"+rslt+"type++++++++++"+type + "results++++++++++++++++"+results);
		if (results != null) {
			flowfile = session.write(flowfile, new StreamCallback() {
				public void process(InputStream in, OutputStream out) throws IOException {
					OutputStreamWriter oss = new OutputStreamWriter(out, "UTF-8");
					log.info("rslt2==========" + rslt);
					oss.write(rslt);
					oss.flush();
					oss.close();
				}
			});
			ls.clear();
			mp.clear();
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

	// combine data to an value;
	public String combineData(String data, int type, int dataNum)
			throws InterruptedException, JSONException, SQLException {
		CombineDataUtil cDataUtil = new CombineDataUtil();
		final ComponentLog log = this.getLogger();
		if(type!=1){
			data = this.regixData(data);
		}
		mp.put(type, data);
		ls.add(type);
		int mSize = mp.size();
		log.info("mSize==========" + mSize);
		int tSize = ls.size();
		log.info("tSize==========" + tSize);
		if ((mSize == dataNum) && (tSize == dataNum)) {
			ls = bubbleSort(ls);
			String combineData = cDataUtil.combineData(ls, mp);
			return combineData;
		}
		return null;
	}

	public String regixData(String data){
		StringBuffer sb = new StringBuffer();
		String str[] = data.split(",");
		for(int i=1;i<str.length;i++){
			sb.append(str[i]);
			if(i!=str.length-1){
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
	public List<Integer> bubbleSort(List<Integer> typeArray) {
		int temp = 0;
		for (int i = 0; i < typeArray.size(); i++) {
			for (int j = i + 1; j < typeArray.size(); j++) {
				if (typeArray.get(i) > typeArray.get(j)) {
					temp = typeArray.get(i);
					typeArray.set(i, typeArray.get(j));
					typeArray.set(j, temp);
				}
			}
		}
		for (int i = 0; i < typeArray.size(); i++)
			System.out.println(typeArray.get(i));
		return typeArray;
	}

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException {

	}
}
