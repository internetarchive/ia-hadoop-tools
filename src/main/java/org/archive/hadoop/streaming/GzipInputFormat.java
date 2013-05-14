package org.archive.hadoop.streaming;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.archive.util.zip.OpenJDK7GZIPInputStream;

public class GzipInputFormat extends TextInputFormat {

	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		
		FileSplit split = (FileSplit)genericSplit;
		
		long start = split.getStart();
		long end = start + split.getLength();
		final Path file = split.getPath();
		end  = Long.MAX_VALUE;
//		compressionCodecs = new CompressionCodecFactory(job);
//		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		
		boolean skipFirstLine = false;
		
		InputStream in = new OpenJDK7GZIPInputStream(fileIn);
		
		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}
		
	    String delimiter = job.get("textinputformat.record.delimiter");
	    byte[] recordDelimiter = null;
	    if (null != delimiter) {
	    	recordDelimiter = delimiter.getBytes();
	    }
		
		if (skipFirstLine) { // skip first line and re-establish "start".
			LineRecordReader.LineReader reader = new LineReader(in, job, recordDelimiter);
			start += reader.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}		
		
		return new LineRecordReader(in, start, end, job, recordDelimiter);

	}
}
