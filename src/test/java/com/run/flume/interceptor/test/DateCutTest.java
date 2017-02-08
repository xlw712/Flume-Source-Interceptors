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

public class DateCutTest {
	private static final Logger logger = LoggerFactory.getLogger(Format4FieldTest.class);
	static Event Event;
	static Context Context;
	//需要格式化的MAC字段名
	public static final String FORMATTED_FIELD = "dateCut";
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
		map.put("RZ002506", "1455667890888");
		Event.setHeaders(map);
		logger.info("构造Event对象完毕");
	}

	@Test
	public void TestInterceptor() {
		String formattedField = Context.getString(FORMATTED_FIELD);
		System.out.println(formattedField);
		
		String[] fields = formattedField.split(SPLITSTRING);
		for(int i=0;i<fields.length;i++){
			System.out.println(fields[i]);
		}
		Map<String, String> mapEvent = Event.getHeaders();
		if(fields.length>0){
			long[] ip = new long[4];  
			for (String strIpName : fields) {
				System.out.println("strIpName"+strIpName);
				String strIpValue = mapEvent.get(strIpName);
				if(strIpValue!=null){
					String str = "";
					if(strIpValue.length()>10){
						str = strIpValue.substring(0, 10);
					}
					System.out.println("str"+str);
				mapEvent.put(strIpName,str);
				}
			}
		}
	}
}
