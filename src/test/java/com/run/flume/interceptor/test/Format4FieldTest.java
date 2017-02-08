package com.run.flume.interceptor.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Format4FieldTest {
	private static final Logger logger = LoggerFactory.getLogger(Format4FieldTest.class);
	static Event Event;
	static Context Context;
	public static final String FORMATTED_FIELD = "formatted4ip";
	public static final String SPLITSTRING = ",";

	@BeforeClass
	public static void initContext() {
		Context = new Context();
		Context.put(FORMATTED_FIELD, "RZ002506");
		logger.info("构造Context对象完毕");
	}

	@BeforeClass
	public static void initEvent() {
		Event = new SimpleEvent();
		Map map = new HashMap<String, String>();
		map.put("RZ002506", "192.168.2.49");
		Event.setHeaders(map);
		logger.info("构造Event对象完毕");
	}

	@Test
	public void TestInterceptor() {
		String formattedField = Context.getString(FORMATTED_FIELD);
		String[] fields = formattedField.split(SPLITSTRING);
		Map<String, String> mapEvent = Event.getHeaders();
		if(fields.length>0){
			long[] ip = new long[4];  
			for (String strIpName : fields) {
				String strIpValue = mapEvent.get(strIpName);
				if(strIpValue!=null){
				//先找到IP地址字符串中.的位置
				int position1 = strIpValue.indexOf(".");  
				int position2 = strIpValue.indexOf(".", position1 + 1);  
				int position3 = strIpValue.indexOf(".", position2 + 1);  
				//将每个.之间的字符串转换成整型
				        ip[0] = Long.parseLong(strIpValue.substring(0, position1));  
				        ip[1] = Long.parseLong(strIpValue.substring(position1+1, position2));  
				        ip[2] = Long.parseLong(strIpValue.substring(position2+1, position3));  
				        ip[3] = Long.parseLong(strIpValue.substring(position3+1));  
				long result = (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];  
				mapEvent.put(strIpName, Long.toString(result));
				}
			}
		}
	}

}
  