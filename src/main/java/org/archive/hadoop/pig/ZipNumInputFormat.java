package org.archive.hadoop.pig;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class ZipNumInputFormat extends NLineInputFormat
{
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public RecordReader createRecordReader(
			InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		
		return new ZipNumRecordReader();
	}
}