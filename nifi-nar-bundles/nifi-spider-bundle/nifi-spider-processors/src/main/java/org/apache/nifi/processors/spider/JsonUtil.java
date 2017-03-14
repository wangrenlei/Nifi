package org.apache.nifi.processors.spider;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonUtil {
	
	public String jsonToStatement(String []argsValues,String dataJson) throws JSONException{
		JSONObject dataObj = new JSONObject(dataJson);
        String optionType = dataObj.getString("type");
        String results = dataObj.getString("results");
		if(Integer.parseInt(optionType)==1){
              System.out.println("type："+optionType+"的数据："+results);
              
              
              return results;
		}else if(Integer.parseInt(optionType)==2){
              System.out.println("type："+optionType+"的数据："+results);
              
              
              
              return  results;  
		}else if(Integer.parseInt(optionType)==3){
            System.out.println("type："+optionType+"的数据："+results);
            
            
            
            return  results;  
		}else {
			return null;
		}
	}
	
	
	public static void main (String args[]) throws JSONException{
		JSONObject objParam = new JSONObject();
//		spider  data
		objParam.put("type", 1);
//		filename,keyword,newsNum
	    objParam.put("results", "江苏吴中实业股份有限公司2013年度报告,江苏吴中实业,2988");
	    
//	    people name
	    objParam.put("type", 2);
//	    filename,people name|people name
	    objParam.put("results", "江苏吴中实业股份有限公司2013年度报告,王大头|路大头|吕大头|梁大头");
	    
//	    financial data
	    objParam.put("type", 3);
//	    filename,year_total_income,last_year_total_income,year_total_cost,last_year_total_cost   
	    objParam.put("results", "江苏吴中实业股份有限公司2013年度报告,18394758,4653728,2037464,3020384");
	}
	
}
