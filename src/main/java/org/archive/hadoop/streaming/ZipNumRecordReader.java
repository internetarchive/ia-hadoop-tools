package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.archive.format.gzip.zipnum.ZipNumCluster;
import org.archive.format.gzip.zipnum.ZipNumParams;
import org.archive.util.iterator.CloseableIterator;

public class ZipNumRecordReader implements RecordReader<LongWritable, Text> {
	
	protected ZipNumCluster cluster = null;
		
	protected CloseableIterator<String> cdxReader;
	
	protected LineRecordReader inner;
	protected ZipNumParams params;
	

	public ZipNumRecordReader(JobConf job, FileSplit fileSplit) throws IOException {
		inner = new LineRecordReader(job, fileSplit);
		
		Path summaryPath = fileSplit.getPath();
		
		String summaryFile = summaryPath.toString();
		
		if (summaryFile.startsWith("file:/")) {
			summaryFile = summaryFile.substring(5);
		}
		
		cluster = new ZipNumCluster();
		cluster.setSummaryFile(summaryFile);
		cluster.init();
		
		params = new ZipNumParams();
		params.setMaxAggregateBlocks(0);
		params.setMaxBlocks(0);		
		cdxReader = cluster.getCDXIterator(new RecordReaderValueIterator(inner), params);
	}
	

	@Override
	public float getProgress() {
		return inner.getProgress();
	}

	@Override
	public synchronized boolean next(LongWritable key, Text value)
			throws IOException {

		if (cdxReader != null && cdxReader.hasNext()) {
			
			String cdxLine = cdxReader.next();
			key.set(this.getPos());
			value.set(cdxLine);	
			return true;
		} else {
			return false;
		}
	}
		
	@Override
	public synchronized void close() throws IOException
	{
		if (cdxReader != null) {
			cdxReader.close();
			cdxReader = null;
		}
		
		inner.close();
	}

	public void seekNear(String key) {
		
		try {
			if (cdxReader != null) {
				cdxReader.close();
				cdxReader = null;
			}
			
			cdxReader = cluster.getCDXIterator(key, null);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public LongWritable createKey() {
		return inner.createKey();
	}


	@Override
	public Text createValue() {
		return inner.createValue();
	}


	@Override
	public long getPos() throws IOException {
		return inner.getPos();
	}
}
