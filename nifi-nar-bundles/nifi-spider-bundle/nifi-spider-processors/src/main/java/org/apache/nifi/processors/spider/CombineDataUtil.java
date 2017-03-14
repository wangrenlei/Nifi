package org.apache.nifi.processors.spider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CombineDataUtil {

	public String combineData(List<Integer> tArray, Map<Integer,String> dataMap) {
		StringBuffer sbBuffer =new StringBuffer();
		for(int i=0;i<tArray.size();i++){
			String str = dataMap.get(tArray.get(i)).toString();
			sbBuffer.append(str);
			if(i!=tArray.size()-1){
				sbBuffer.append(",");
			}
		}
		System.out.println("合并后的字符串："+sbBuffer.toString());
		return sbBuffer.toString();
	}


	public static void main(String args[]) {
		List l = new ArrayList<>();
		l.add(99);
		l.add(1);
		l.add(3);
		l.add(2);
		
		Map<Integer,String> dataMap = new HashMap<Integer,String>();
		dataMap.put(1, "人名提取");
		System.out.println("unit test！");
	}
}
