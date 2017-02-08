package com.run.flume.interceptor.test;

import java.lang.reflect.Method;
import java.util.Arrays;
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

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

public class Map2Ayena_BasicTest {
	private static final Logger logger = LoggerFactory.getLogger(Map2Ayena_BasicTest.class);
	Event Event;
	Context Context;
	/*
	 * 包名类路径
	 */
	public static final String CONTEXT_REFLECT_PATH = "packagetPath";
	/*
	 * 表名
	 */
	public static final String CONTEXT_REFLECT_DATASPACE = "dataspace";
	/*
	 * 执行的方法名
	 */
	public static final String METHOD_NAME_NEWBUILDER = "newBuilder";

	@Before
	public void initContext() {
		Context = new Context();
		Context.put(CONTEXT_REFLECT_DATASPACE, "S003.RWA_BASIC_AUTH_AIRPORT");
		Context.put(CONTEXT_REFLECT_PATH, "S003.RWA_BASIC_AUTH_AIRPORT_$RWA_BASIC_AUTH_AIRPORT");
		logger.info("构造Context对象完毕");
	}

	@Before
	public void initEvent() {
		Event = new SimpleEvent();
		Map map = new HashMap<String, String>();
		map.put("RI070006", "98:F1:70:2B:09:94");
		map.put("RB030004", "163");
		map.put("RB030005", "15183955879");
		map.put("RH010014", "1463528001000");
		map.put("RH010015", "1463531700");
		map.put("RH010016", "1463530931");
		map.put("RH010036", "510106");
		map.put("RZ002001", "1111");
		Event.setHeaders(map);
		logger.info("构造Event对象完毕");
	}

	@Test
	public void TestInterceptor() {
		Class cls;
		Method gmMethod;
		GeneratedMessage gm;
		try {
			logger.info("通过反射得到protobuf序列化对象");
			cls = Class.forName(Context.getString(CONTEXT_REFLECT_PATH));
			gmMethod = cls.getDeclaredMethod("newBuilder", null);
			com.google.protobuf.GeneratedMessage.Builder builder = (com.google.protobuf.GeneratedMessage.Builder) gmMethod
					.invoke(cls, null);
			logger.info("通过EventHeaders中存在的字段,将protobuf填充");
			Map<String, String> mapEvent = Event.getHeaders();
			Method[] methos = builder.getClass().getMethods();
			for (Method method2 : methos) {
				logger.info("获取方法名以set为首,不以Bytes结尾,不包含Field的方法");
				if (method2.getName().startsWith("set") && !method2.getName().endsWith("Bytes")
						&& !method2.getName().contains("Field")) {
					String value = mapEvent.get(method2.getName().substring(3));

					switch (method2.getParameterTypes()[0].getName()) {
					case "long":
						method2.invoke(builder, value == null ? 0 : Long.parseLong(value));
						break;
					default:
						method2.invoke(builder, value == null ? "" : value);
						break;

					}
				}
			}
			gm = (GeneratedMessage) builder.build();
			byte[] byteArray = gm.toByteArray();
			logger.info("构造Ayena头格式");
			byte[] keyBytes = Context.getString(CONTEXT_REFLECT_DATASPACE).getBytes();
			byte[] value = new byte[keyBytes.length + 1 + byteArray.length];
			value[0] = (byte) keyBytes.length;
			System.arraycopy(keyBytes, 0, value, 1, keyBytes.length);
			System.arraycopy(byteArray, 0, value, keyBytes.length + 1, byteArray.length);
			logger.info(new String(subBytes(value, 0, value[0] + 1)));
			int a = value[0];
			byte[] msgK = new byte[a];
			System.arraycopy(value, 1, msgK, 0, a);
			String key = new String(msgK);
			String namespace = key.substring(0, key.lastIndexOf("."));
			String dataset = key.substring(key.lastIndexOf(".") + 1);
			Event.setBody(value);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		S003.RWA_BASIC_AUTH_AIRPORT_.RWA_BASIC_AUTH_AIRPORT builder;
		byte[] bodyBytes = Event.getBody();
		logger.info("Ayena头部分字节长度");
		int i = bodyBytes[0] + 1;
		try {
			builder = S003.RWA_BASIC_AUTH_AIRPORT_.RWA_BASIC_AUTH_AIRPORT
					.parseFrom(Arrays.copyOfRange(bodyBytes, i, bodyBytes.length));
			Assert.assertEquals(builder.getRI070006(), "98:F1:70:2B:09:94");
			Assert.assertEquals(builder.getRB030004(), "163");
			Assert.assertEquals(builder.getRB030005(), "15183955879");
			Assert.assertEquals(builder.getRH010014(), 1463528001000L);
			Assert.assertEquals(builder.getRH010015(), "1463531700");
			Assert.assertEquals(builder.getRH010016(), 1463530931L);
			Assert.assertEquals(builder.getRH010036(), 510106L);
		} catch (InvalidProtocolBufferException e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * 从一个byte[]数组中截取一部分
	 * 
	 * @param src
	 * @param begin
	 * @param count
	 * @return
	 */
	public static byte[] subBytes(byte[] src, int begin, int count) {
		byte[] bs = new byte[count];
		for (int i = begin; i < begin + count; i++)
			bs[i - begin] = src[i];
		return bs;
	}
}
