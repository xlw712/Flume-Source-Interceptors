package com.run.flume.interceptor;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

/**
 * Map2Ayena通用类
 * 
 * @author xLw
 * @since JDK 1.7
 * @date 2016年6月3日 下午5:50:07
 */
public class Map2Ayena_Basic implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(Map2Ayena_Basic.class);
	Context context;

	public Map2Ayena_Basic(Context context) {
		this.context = context;
	}
	@Override
	public void initialize() {

	}

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
	@Override
	public Event intercept(Event event) {
		Class cls;
		Method method;
		try {
			cls = Class.forName(context.getString(CONTEXT_REFLECT_PATH));
			method = cls.getDeclaredMethod(METHOD_NAME_NEWBUILDER, null);
			com.google.protobuf.GeneratedMessage.Builder builder = (com.google.protobuf.GeneratedMessage.Builder) method
					.invoke(cls, null);
			Map<String, String> mapEvent = event.getHeaders();
			Method[] methos = builder.getClass().getMethods();
			logger.debug("获取方法名以set为首,不以Bytes结尾,不包含Field的方法");
			for (Method method2 : methos) {
				if (method2.getName().startsWith("set") && !method2.getName().endsWith("Bytes")
						&& !method2.getName().contains("Field")) {
					String value = mapEvent.get(method2.getName().substring(3));
					switch (method2.getParameterTypes()[0].getName()) {
					case "long":
						method2.invoke(builder, value==null? 0 : Long.parseLong(value));
						break;
					default:
						method2.invoke(builder, value==null? "" : value);
						break;
					}
				}
			}
			GeneratedMessage gm = (GeneratedMessage) builder.build();
			byte[] byteArray = gm.toByteArray();
			byte[] keyBytes = context.getString(CONTEXT_REFLECT_DATASPACE).getBytes();

			byte[] value = new byte[keyBytes.length + 1 + byteArray.length];
			value[0] = (byte) keyBytes.length;
			System.arraycopy(keyBytes, 0, value, 1, keyBytes.length);
			System.arraycopy(byteArray, 0, value, keyBytes.length + 1, byteArray.length);
			event.setBody(value);
		} catch (ClassNotFoundException e) {
			logger.error("类加载器找不到对应的类路径", e);
		} catch (NoSuchMethodException e) {
			logger.error("反射过程中没有找到方法", e);
		} catch (Exception e) {
			logger.error("反射调用方法时出错", e);
		}
		return event;
	}
	@Override
	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}
	@Override
	public void close() {

	}

	public static class Builder implements Interceptor.Builder {

		public Context context;

		public void configure(Context context) {
			this.context = context;
		}

		public Interceptor build() {
			return new Map2Ayena_Basic(context);
		}

	}
}
