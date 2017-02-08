package com.run.flume.interceptor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.common.base.Preconditions;

public class CompositeID implements Interceptor {

	StringBuffer tmp = new StringBuffer();
	
	private String[] headers;
	private String idName;
	
	private MessageDigest md;
	public CompositeID(Context context) {
		Preconditions.checkArgument(!StringUtils.isEmpty(context.getString(Constants.HEADERS)),
		          "Must supply a valid headers string");
		Preconditions.checkArgument(!StringUtils.isEmpty(context.getString(Constants.IDName)),
		          "Must supply a valid idName string");
		this.headers = context.getString(Constants.HEADERS).split(",");
		this.idName = context.getString(Constants.IDName);
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			
		}
	}

	public void initialize() {

	}

	public Event intercept(Event event) {
		tmp.setLength(0);
		Map<String, String> headers2 = event.getHeaders();
		//按照指定的顺序拼接各字段值
		for(String header : headers) {
			tmp.append(headers2.get(header));
		}
		//计算拼接完的MD5值
		md.update(tmp.toString().getBytes());
		byte[] digest = md.digest();
		tmp.setLength(0);
		for(int offset=0; offset<digest.length; offset++) {
			String hex = Integer.toHexString(0XFF & digest[offset]);
			if(hex.length()==1)
				tmp.append('0');
			tmp.append(hex);
		}
		//将计算出来的MD5值，添加到指定字段上
		headers2.put(idName, tmp.toString());
		event.setHeaders(headers2);
		
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		for(Event event : events) {
			intercept(event);
		}
		return events;
	}

	public void close() {
		// TODO Auto-generated method stub

	}
	
	public static class Builder implements Interceptor.Builder {

		public Context context;
		
		public void configure(Context context) {
			this.context = context;
		}

		public Interceptor build() {
			return new CompositeID(context);
		}
		
	}
	
	public static class Constants {
		public static String HEADERS = "headers";
		public static String IDName = "idName";
	}

}
