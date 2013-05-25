package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class ZipNumRecordWriter extends org.archive.hadoop.mapreduce.ZipNumRecordWriter implements RecordWriter<Text, Text>{

	public ZipNumRecordWriter(CompressionCodec codec, FSDataOutputStream out,
			FSDataOutputStream summary, String partitionName, long limit)
			throws IOException {
		super(codec, out, summary, partitionName, limit);
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		super.close(null);		
	}
}
