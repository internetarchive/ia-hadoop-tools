package org.archive.cassandra;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.archive.format.cdx.StandardCDXLineFactory;

public class CassCDXRecordWriter extends RecordWriter<Text, Text> {

	protected CDXImporter importer;
	
	public CassCDXRecordWriter(TaskAttemptContext context)
	{
		Configuration conf = context.getConfiguration();
		
		String nodehost = conf.get("conf.cass.host");
		importer = new CDXImporter();
		
		String query = conf.get("conf.cass.query");
		if (query != null) {
			importer.setCdxQuery(query);
		}
		
		String cdxFormat = conf.get("conf.cass.cdxformat");
		if (cdxFormat != null) {
			importer.setCdxLineFactory(new StandardCDXLineFactory(cdxFormat));
		}
		
		int batchSize = conf.getInt("conf.cass.batchSize", -1);
		if (batchSize > 0) {
			importer.setNumToBatch(batchSize);
		}
		
		int minuteTimeout = conf.getInt("conf.cass.minuteTimeout", -1);
		if (minuteTimeout > 0) {
			importer.setMinuteTimeout(minuteTimeout);
		}
		
		boolean canon = conf.getBoolean("conf.cass.canon", false);
		
		importer.init(nodehost, context, canon);
	}
	
	@Override
    public void write(Text key, Text value) throws IOException,
            InterruptedException {
		
		String cdxline;
		
	    if (value.getLength() == 0) {
	        cdxline = key.toString();
	    } else if (key.getLength() == 0) {
	    	cdxline = value.toString();
	    } else {
	    	cdxline = key + " " + value;
	    }
	    	    
	    importer.insertCdxLine(cdxline);
    }

	@Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
		
		importer.close();
    }
}
