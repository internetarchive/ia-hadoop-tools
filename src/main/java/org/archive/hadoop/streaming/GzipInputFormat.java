package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class GzipInputFormat extends FileInputFormat<Text, Text> {
	
	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		
		FileSplit fileSplit = (FileSplit)split;
		
		return new GzipSingleFileRecordReader(fileSplit.getPath(), 0, job);
	}
}
