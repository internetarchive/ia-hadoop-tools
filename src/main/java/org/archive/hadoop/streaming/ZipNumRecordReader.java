package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.archive.format.gzip.zipnum.ZipNumIndex;
import org.archive.format.gzip.zipnum.ZipNumParams;
import org.archive.util.iterator.CloseableIterator;

public class ZipNumRecordReader implements RecordReader<Text, Text> {
	
	protected ZipNumIndex cluster = null;
		
	protected CloseableIterator<String> cdxReader;
	
	protected LineRecordReader inner;
	protected ZipNumParams params;
	
	public ZipNumRecordReader(CombineFileSplit combineSplit, Configuration conf, Reporter reporter, Integer index) throws IOException
	{
		Path path = combineSplit.getPath(index);
		long start = combineSplit.getOffset(index);
		long length = combineSplit.getLength(index);
		String[] locs = combineSplit.getLocations();
		
		init(conf, new FileSplit(path, start, length, locs));
	}
	

	public ZipNumRecordReader(JobConf job, FileSplit fileSplit) throws IOException {
		init(job, fileSplit);
	}
	
	protected void init(Configuration conf, FileSplit fileSplit) throws IOException
	{
		inner = new LineRecordReader(conf, fileSplit);
		
		String summaryFile = conf.get("conf.zipnum.locationPath", null);
		
		if (summaryFile == null) {
			Path summaryPath = fileSplit.getPath();
			summaryFile = summaryPath.toString();
		}
		
		if (!summaryFile.contains(":/")) {
			summaryFile = conf.get(FileSystem.FS_DEFAULT_NAME_KEY, "") + summaryFile;
		}
		
		cluster = ZipNumIndex.createIndexWithSummaryPath(summaryFile);
		
		params = new ZipNumParams();
		params.setMaxAggregateBlocks(conf.getInt("conf.zipnum.maxAggBlocks", 3000));
		params.setMaxBlocks(0);
		
		cdxReader = cluster.getCDXIterator(new RecordReaderValueIterator(inner), params);
	}
	

	@Override
	public float getProgress() throws IOException {
		return inner.getProgress();
	}

	@Override
	public synchronized boolean next(Text key, Text value)
			throws IOException {

		if (cdxReader != null && cdxReader.hasNext()) {
			
			String cdxLine = cdxReader.next();
			key.set(cdxLine);
			value.set("");	
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
	public Text createKey() {
		return new Text();
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
