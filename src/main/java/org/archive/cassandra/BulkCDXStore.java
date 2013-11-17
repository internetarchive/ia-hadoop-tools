package org.archive.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.hadoop.BulkOutputFormat;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.archive.format.cdx.CDXLine;
import org.archive.format.cdx.StandardCDXLineFactory;

public class BulkCDXStore extends StoreFunc  {
	
	RecordWriter<ByteBuffer,List<Mutation>> writer;
	
	@Override
    public OutputFormat getOutputFormat() throws IOException {
	    // TODO Auto-generated method stub
	    return new BulkOutputFormat();
    }

	@Override
    public void setStoreLocation(String location, Job job) throws IOException {
	    // TODO Auto-generated method stub
	    
    }

	@Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
    }

	@Override
    public void putNext(Tuple t) throws IOException {
		String line = concatTuple(t);
		CDXLine cdxline = new CDXLine(line, StandardCDXLineFactory.cdx11);
		
		ByteBuffer key = ByteBufferUtil.bytes(cdxline.getUrlKey());
		
		long timestamp = System.currentTimeMillis();
		List<Mutation> muts = new ArrayList<Mutation>();
		
		muts.add(createMut("datetime", cdxline.getTimestamp(), timestamp));
		
		muts.add(createMut("originalurl", cdxline.getOriginalUrl(), timestamp));
		muts.add(createMut("mimetype", cdxline.getMimeType(), timestamp));
		muts.add(createMut("digest", cdxline.getDigest(), timestamp));
		
		muts.add(createMut("statuscode", NumberUtils.toInt(cdxline.getStatusCode(), -1), timestamp));
		muts.add(createMut("length", NumberUtils.toInt(cdxline.getLength(), -1), timestamp));
		muts.add(createMut("offset", NumberUtils.toInt(cdxline.getOffset(), 0), timestamp));
		
		muts.add(createMut("filename", cdxline.getFilename(), timestamp));
		
		try {
	        writer.write(key, muts);
        } catch (InterruptedException ie) {
        	throw new IOException(ie);
        }
    }
	
	protected Mutation createMut(String name, String value, long timestamp)
	{
		return createMut(name, ByteBufferUtil.bytes(value), timestamp);
	}
	
	protected Mutation createMut(String name, long value, long timestamp)
	{
		return createMut(name, ByteBufferUtil.bytes(value), timestamp);
	}
	
	protected Mutation createMut(String name, int value, long timestamp)
	{
		return createMut(name, ByteBufferUtil.bytes(value), timestamp);
	}
	
	protected Mutation createMut(String name, ByteBuffer value, long timestamp)
	{
		Column col = new Column();
		col.setName(ByteBufferUtil.bytes(name));
		col.setValue(value);
		col.setTimestamp(timestamp);
		 
		Mutation mut = new Mutation();
		mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(col));
		return mut;
	}
	
	String concatTuple(Tuple t) throws ExecException
	{
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < t.size(); i++) {
			if (i > 0) {
				sb.append(' ');
			}
			sb.append(t.get(i).toString());
		}
		return sb.toString();
	}
}
