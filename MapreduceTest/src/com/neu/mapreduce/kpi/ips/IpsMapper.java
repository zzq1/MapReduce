package com.neu.mapreduce.kpi.ips;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mapreduce.kpi.Kpi;

public class IpsMapper extends Mapper<Object, Text, Text, Text>{

	Text requestPage = new Text();// 网页地址
    Text remoteAddr = new Text();// 用户ip地址
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Kpi kpi = Kpi.parse(value.toString());
		if(kpi.getIs_validate()) {
			requestPage.set(kpi.getRequest_page());
			remoteAddr.set(kpi.getRemote_addr());
			context.write(requestPage, remoteAddr);
		}
	}
}
