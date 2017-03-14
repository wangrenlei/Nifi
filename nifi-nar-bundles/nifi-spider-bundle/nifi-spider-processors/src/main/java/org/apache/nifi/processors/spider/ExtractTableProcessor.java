package org.apache.nifi.processors.spider;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONException;
import org.json.JSONObject;

@Tags({"报表"})
@CapabilityDescription("从文本文件中提取指定信息报表.")
public class ExtractTableProcessor extends AbstractProcessor{
	private static char[] NOTEMARK = {'一','二','三','四','五','六','七','八','九','十','、','1','2','3','4','5','6','7','8','9','0','附','注'};
	
	public static final PropertyDescriptor TABLENAME = new PropertyDescriptor.Builder()
			.name("input table_name").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final Relationship SUCCESS = new Relationship.Builder()
			.name("SUCCESS").description("Succes Output").build();
	
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	
	@Override
	protected void init(ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(TABLENAME);
		this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
	}
	
	@Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }
	
	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowfile = session.get();
        if ( flowfile == null ) {
        	flowfile = session.create();
        }
        Map<PropertyDescriptor, String> props = context.getProperties();
		String table_name = props.get(TABLENAME); 
		//读取文件提取表格行内容存放list
        List<String> list = new ArrayList<>();
        session.read(flowfile, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
				BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
				String line = null;
	            boolean flag = false;
	            while((line = br.readLine()) != null){
	                if(flag == false){
	                    if(line.contains(table_name) && line.trim().length() < table_name.toCharArray().length + 3){
	                        flag = true;
	                    }
	                }else if(( line.contains("会计机构负责人") || line.contains("后附财务报表附注") )&& flag == true){
	                    flag = false;
	                }else{
	                	list.add(line);
	                }
	            }
	            br.close();
	            in.close();
			}
		});
        //处理list提取结果字段存放map
	    Map<String, String> mapRes = new HashMap<>();
		if(formateTable(list) != null && list.size() > 0){
			mapRes = extractTable(list);
		}
		//拼接返回结果obj
		String result = flowfile.getAttribute("filename") + ","
				+ (mapRes.get("year_total_income")==null?"0":mapRes.get("year_total_income")) + ","
				+ (mapRes.get("last_year_total_income")==null?"0":mapRes.get("last_year_total_income")) + ","
				+ (mapRes.get("year_total_cost")==null?"0":mapRes.get("year_total_cost")) + ","
				+ (mapRes.get("last_year_total_cost")==null?"0":mapRes.get("last_year_total_cost"));
		JSONObject obj = new JSONObject();
		try {
			obj.put("type", 2);
			obj.put("results", result);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//返回结果写入flowfile
        flowfile = session.write(flowfile, new StreamCallback() {
			public void process(InputStream in, OutputStream out)
					throws IOException {
				OutputStreamWriter oss = new OutputStreamWriter(out, "UTF-8");
				oss.write(obj.toString());
				oss.flush();
				oss.close();
			}
		});
        session.transfer(flowfile, SUCCESS);
	}
	
	public List<String> formateTable(List<String> list){
        if(list.size() > 4){
            boolean flag_1 = false;
            boolean flag_2 = false;
            for(int i = 0; i < 5; i++){
                if(list.get(i).contains("单位")){
                    flag_1 = true;
                    continue;
                }
                if(list.get(i).contains("额") && list.get(i).split(" ").length > 2){
                    flag_2 = true;
                    continue;
                }
            }
            if(flag_1 == true && flag_2 == true){
                return list;
            }
        }
        return null;
    }
	
	public Map<String, String> extractTable(List<String> list){
		Map<String, String> map = new HashMap<>();
		boolean flag = false;
		for(int i = 0; i < list.size(); i++){
            String line = list.get(i).trim().replace(",", "").replaceAll("\\s+", " ");
            if("".equals(line.trim()) || line.contains("年度报告") || line.trim().matches("^[1-9]\\d*$")){
                continue;
            }
            String afterLine = "";
            if(i < list.size() - 1){
                afterLine = list.get(i + 1);
            }
            String[] str = line.split(" ");
            if(flag == false){
                for(int j = 0; j < str.length; j++){
                    if(str[j].contains("额") && str[j].length() == 4){
                        flag = true;
                        break;
                    }
                }
                if(flag == true){
                    continue;
                }
            }else{
                String writeLine = line;
                if(str.length > 1 && isNote(str[1])){
                    writeLine = line.replace(str[1], "");
                }
                writeLine = writeLine.replace(" ", ",").replace(",,", ",") + ",";
//                res = res + writeLine;
                //取四个值返回
                String[] lineArr = writeLine.split(",");
                if(lineArr.length > 2){
                	if(lineArr[0].contains("营业总收入")){
                		map.put("year_total_income", lineArr[1]);
                		map.put("last_year_total_income", lineArr[2]);
                	}else if(lineArr[0].contains("营业总成本")){
                		map.put("year_total_cost", lineArr[1]);
                		map.put("last_year_total_cost", lineArr[2]);
                	}
                }
                if(" ".equals(afterLine) && !line.endsWith("：") && !Pattern.compile("[0-9]+?").matcher(writeLine).find()){
                }else{
//                	res = res + "\n";
                }
            }
        }
		return map;
	}
	
	public boolean isNote(String str){
        char[] cArr = str.toCharArray();
        boolean flag = true;
        for(char c : cArr){
            boolean cflag = false;
            for(int i = 0; i < NOTEMARK.length; i++){
                if(c == NOTEMARK[i]){
                    cflag = true;
                    break;
                }
            }
            if(cflag == false){
                flag = false;
                break;
            }
        }
        return flag;
    }

	public void close(Closeable close){
        if(close != null){
            try {
                close.close();
            } catch (IOException ex) {
            	ex.printStackTrace();
            }
        }
    }
	
}
