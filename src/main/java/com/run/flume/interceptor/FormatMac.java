package com.run.flume.interceptor;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatMac implements Interceptor{
	private static final Logger logger = LoggerFactory.getLogger(FormatMac.class);
	Context context;

	
	public FormatMac(Context context) {
		this.context = context;
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}
	/**
	 * 闇�瑕佹棩鏈熸牸寮忓寲鐨勫瓧娈靛悕锛屽涓瓧娈典娇鐢ㄩ�楀彿鍒嗛殧
	 */
	
	public static final String FORMATTED_FIELD = "formattedMac";
	public static final String SPLITSTRING = ",";
	@Override
	public Event intercept(Event event) {
		//鑾峰彇
				String formattedField = context.getString(FORMATTED_FIELD);
				String[] fields = formattedField.split(SPLITSTRING);
				if(fields.length>0){
					long[] ip = new long[4];  
					for (String strIpName : fields) {
						String strIpValue = event.getHeaders().get(strIpName);
						if(strIpValue!=null){
							String fields1 = strIpValue.replaceAll("-", ":");
							String fieldsUpper = fields1.toUpperCase();
							Map<String, String> headers = event.getHeaders();
							headers.put(strIpName,fieldsUpper);
						}
					}
				}
				return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		for(Event event : events) {
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
			return new FormatMac(context);
		}
		
	}
	
}
