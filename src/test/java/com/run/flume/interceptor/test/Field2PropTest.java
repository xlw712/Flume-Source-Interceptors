package com.run.flume.interceptor.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.flume.utils.PropertiesUtil;

public class Field2PropTest {
	private static final Logger logger = LoggerFactory.getLogger(Field2PropTest.class);
	/**
	 * 需要映射的字段
	 */
	public static final String CONTEXT_FIELD = "field";
	/**
	 * 根据properties文件映射路径
	 */
	public static final String CONTEXT_CAUSEPATH = "causePath";
	Event event;
	Context context;
	PropertiesUtil prop;

	@Before
	public void initContext() {
		context = new Context();
		context.put(CONTEXT_FIELD, "RH010036");
		context.put(CONTEXT_CAUSEPATH, "src/main/resources/config.properties");
		logger.info("构造Context对象完毕");
	}

	@Before
	public void initEvent() {
		event = new SimpleEvent();
		Map map = new HashMap<String, String>();
		map.put("RI070006", "98:F1:70:2B:09:94");
		map.put("RB030004", "163");
		map.put("RB030005", "15183955879");
		map.put("RH010014", "1463528001000");
		map.put("RH010015", "1463531700");
		map.put("RH010016", "1463530931");
		map.put("RH010036", "510106");
		map.put("RZ002001", "1111");
		event.setHeaders(map);
		logger.info("构造Event对象完毕");
	}

	@Before
	public void initialize() {
		if (context.getString(CONTEXT_CAUSEPATH)==null) {
			throw new NullPointerException(CONTEXT_CAUSEPATH+" command is Not Found !");
		}
		prop = new PropertiesUtil(context.getString(CONTEXT_CAUSEPATH));
	}

	@Test
	public void TestInterceptor() {
		Map<String, String> map = event.getHeaders();
		String field = context.getString(CONTEXT_FIELD);
		if (map.get(field) != null && prop.getProperty(map.get(field)) != null) {
			map.put(field, prop.getProperty(map.get(field)));
		}
		for (Map.Entry<String, String> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
		logger.info("校验转换后的字段是否正确");
		Map<String,String> returnMap=event.getHeaders();
		Assert.assertEquals(returnMap.get("RH010036"),"成都北站");
	}
}
