package com.run.flume.interceptor;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.flume.utils.PropertiesUtil;

/**
 * 
 * 类描述：Field字段转换为Properties中对应的值
 * 
 * @Description: TODO
 * @author xLw
 * @since JDK 1.7
 * @date 2016年6月12日 下午1:07:30
 */
public class Field2Prop implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(Field2Prop.class);
	/**
	 * 需要映射的字段
	 */
	public static final String CONTEXT_FIELD = "field";
	/**
	 * 根据properties文件映射路径
	 */
	public static final String CONTEXT_CAUSEPATH = "causePath";
	Context context;
	PropertiesUtil prop;

	public Field2Prop(Context context) {
		this.context = context;
	}

	@Override
	public void initialize() {
		if (context.getString(CONTEXT_CAUSEPATH) == null) {
			throw new NullPointerException(CONTEXT_CAUSEPATH + " command is Not Found !");
		}
		prop = new PropertiesUtil(context.getString(CONTEXT_CAUSEPATH));
	}

	@Override
	public Event intercept(Event event) {
		Map<String, String> map = event.getHeaders();
		String field = context.getString(CONTEXT_FIELD);
		if (field == null) {
			throw new NullPointerException(CONTEXT_FIELD + " command is Not Found !");
		}
		if (field == null) {
		}
		if (map.get(field) != null && prop.getProperty(map.get(field)) != null) {
			map.put(field, prop.getProperty(map.get(field)));
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
			return new Field2Prop(context);
		}

	}
}
