package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class CombinedGzipInputFormat extends CombineFileInputFormat<Text, Text> {
	
	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		
		 return new CombineFileRecordReader<Text, Text>(job, (CombineFileSplit) split, reporter, (Class)GzipSingleFileRecordReader.class);
	}
}
