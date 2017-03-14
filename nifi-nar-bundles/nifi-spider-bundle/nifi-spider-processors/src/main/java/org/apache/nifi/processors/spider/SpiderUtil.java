package org.apache.nifi.processors.spider;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.codehaus.jettison.json.JSONObject;

import com.renet.feed.crawler.ScriptStoreImpl;
import com.renet.feed.crawler.SnsCrawler;

public class SpiderUtil {
	public static long getMention(String keyword) {
		SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.US);
		ScriptStoreImpl script = null;
		String os = System.getProperty("os.name");
        if (os.toLowerCase().indexOf("windows") >= 0) {
//        	script = new ScriptStoreImpl("E:\\nifi\\");
        	script = new ScriptStoreImpl("./src/main/resources/");
        } else{
        	script = new ScriptStoreImpl("./src/main/resources/");
//        	script = new ScriptStoreImpl("/home/rennet/");
        }
		SnsCrawler sc = new SnsCrawler(script);
		long mention = -1;
		try {
			sc.initSNS("news_baidu_com", " ", " ", "utf-8");
			String info = sc.fetchSeedsTotal(keyword, "Mon, 01 Jan 1900 00:00:00 CST", format.format(new Date()));
			JSONObject obj = new JSONObject(info);
			mention = obj.optLong("mention");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mention;
	}
	
	public static void main(String args[]){
		long a = new SpiderUtil().getMention("苏州高新区高新技术产业股份有限公司");
		System.out.println(a);
	}
}
