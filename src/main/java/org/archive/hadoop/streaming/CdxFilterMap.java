package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CdxFilterMap implements Mapper<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	String[] startlines = {" CDX", "dns:", "filedesc:", "warcinfo:" };

	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		
		String cdx = key.toString();
		
		for (String startline : startlines) {
			if (cdx.startsWith(startline)) {
				return;
			}
		}
		
		output.collect(key, value);
	}
}
