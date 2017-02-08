package com.run.flume.interceptor.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Before;
import org.junit.Test;

import com.run.flume.interceptor.CompositeID.Builder;
import com.run.flume.interceptor.CompositeID.Constants;

import junit.framework.Assert;

public class CompositeIDTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		Context context = new Context();
		context.put(Constants.HEADERS, "a,b,c,d");
		context.put(Constants.IDName, "id");
		Interceptor build = build(context);

		// 基础测试
		List<Event> events = new ArrayList<Event>();
		Event event1 = new SimpleEvent();
		HashMap<String, String> headers = new HashMap<String, String>();
		headers.put("a", "1");
		headers.put("c", "2");
		headers.put("d", "3");
		headers.put("e", "4");

		event1.setHeaders(headers);
		events.add(event1);
		List<Event> intercept = build.intercept(events);

		Map<String, String> headers2 = (intercept.get(0)).getHeaders();
		String string = headers2.get(context.getString(Constants.IDName));
		Assert.assertEquals(true, string != null);
		Assert.assertEquals(32, string.length());
		System.out.println(string);
	}

	private Interceptor build(Context context) {
		Builder builder = new Builder();
		builder.configure(context);
		return builder.build();
	}
}
