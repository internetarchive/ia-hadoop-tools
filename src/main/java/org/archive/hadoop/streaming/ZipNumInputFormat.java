package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NLineInputFormat;


public class ZipNumInputFormat extends NLineInputFormat
{
	@SuppressWarnings("unchecked")
	@Override
	public RecordReader getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		
	    reporter.setStatus(genericSplit.toString());
	    return new ZipNumRecordReader(job, (FileSplit) genericSplit);
	}	
}