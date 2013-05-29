package org.archive.hadoop.streaming;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CdxDedupReducer implements Reducer<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	protected String lastCdx = null;

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String cdx = key.toString();
		
		if (lastCdx != null && lastCdx.equals(cdx)) {
			return;
		}
		
	    while (values.hasNext()) {
	        output.collect(key, values.next());
	    }
	    
	    lastCdx = cdx;
	}
}
