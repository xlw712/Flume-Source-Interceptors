package com.run.flume.interceptor;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 入库字段格式化
 * @author lixueping   
 * @date 2016年6月7日 下午2:12:49
 */
public class Format4IP implements Interceptor{
	private static final Logger logger = LoggerFactory.getLogger(Format4IP.class);
	Context context;

	
	public Format4IP(Context context) {
		this.context = context;
	}

	public void initialize() {
	}
	/**
	 * 需要日期格式化的字段名，多个字段使用逗号分隔
	 */
	
	public static final String FORMATTED_FIELD = "formatted4ip";
	public static final String SPLITSTRING = ",";

	public Event intercept(Event event) {
		//获取
		String formattedField = context.getString(FORMATTED_FIELD);
		String[] fields = formattedField.split(SPLITSTRING);
		if(fields.length>0){
			long[] ip = new long[4];  
			for (String strIpName : fields) {
				String strIpValue = event.getHeaders().get(strIpName);
				System.out.println("666666666"+strIpValue);
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
					Map<String, String> headers = event.getHeaders();
					headers.put(strIpName, Long.toString(result));
				}
			}
		}
		return event;
	}



	public List<Event> intercept(List<Event> events) {
		for(Event event : events) {
			intercept(event);
		}
		return events;
	}

	public void close() {

	}
	
	public static class Builder implements Interceptor.Builder {

		public Context context;
		
		public void configure(Context context) {
			this.context = context;
		}

		public Interceptor build() {
			return new Format4IP(context);
		}
		
	}
	
	

}
  