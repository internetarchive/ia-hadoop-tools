package org.archive.cassandra;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.archive.format.cdx.StandardCDXLineFactory;

public class CassCDXRecordWriter extends RecordWriter<Text, Text> {

	protected CDXImporter importer;
	protected int count = 0;
	
	public CassCDXRecordWriter(Configuration conf)
	{
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
		
		importer.init(nodehost);
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
	    
	    ++count;
	    
	    if ((count % 10000) == 0) {
	    	System.out.println(count);
	    }
	    
	    importer.insertCdxLine(cdxline);
    }

	@Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
		
		importer.close();
    }
}
