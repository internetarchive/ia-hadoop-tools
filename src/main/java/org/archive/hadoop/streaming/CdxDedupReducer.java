package org.archive.hadoop.streaming;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	Pattern youtube = Pattern.compile("^com,youtube[,)].*/videoplayback([?]|.+&)id=([^&]+).*");

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
	    
		if (cdx.startsWith("com,youtube")) {
			checkVideo(cdx, output);
		}
	}
	
	protected void checkVideo(String cdx, OutputCollector<Text, Text> output) throws IOException
	{
		String[] fields = cdx.split(" ");
		
		if (fields.length < 5) {
			return;
		}
		
		if (fields[4].equals("200") && fields[3].startsWith("video/")) {
			Matcher m = youtube.matcher(cdx);
			if (m.matches()) {
				String result = m.replaceFirst("youtube,derived)/$2") + cdx.substring(fields[0].length());
				output.collect(new Text(result), new Text());
			}
		}

	}
}
