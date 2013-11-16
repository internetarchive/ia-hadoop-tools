package org.archive.cassandra;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

public class CassCDXStore extends StoreFunc {
	
	protected CassCDXRecordWriter cassWriter;
	protected Text key   = new Text();
	protected Text value = new Text();

	@Override
    public OutputFormat<Text, Text> getOutputFormat() throws IOException {
		return new CassCDXOutputFormat();
    }

	@Override
    public void setStoreLocation(String location, Job job) throws IOException {
	    // TODO Auto-generated method stub
	    
    }

	@SuppressWarnings("rawtypes")
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
		cassWriter = (CassCDXRecordWriter)writer;
	    
    }

	@Override
    public void putNext(Tuple t) throws IOException {
		
		int size = t.size();
		
		if (size != 2 && size != 1) {
            throw new IOException("Invalid tuple size, must be 1 or 2: " + size);
		}

		key.set(t.get(0).toString());
		
		if (size == 1) {
			value.set("");
		} else {
			value.set(t.get(1).toString());
		}
		
		try {
	        cassWriter.write(key, value);
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }
}
