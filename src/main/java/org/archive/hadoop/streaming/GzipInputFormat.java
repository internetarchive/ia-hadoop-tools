package org.archive.hadoop.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class GzipInputFormat extends CombineFileInputFormat<LongWritable, Text> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		
		CombineFileSplit split = (CombineFileSplit)genericSplit;
		
		Vector<InputStream> vec = new Vector<InputStream>();
		
		for (Path path : split.getPaths()) {
			FileSystem fs = path.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(path);
			vec.add(fileIn);
		}
		
		InputStream in = new SequenceInputStream(vec.elements());
		
//		long start = split.getStart();
//		long end = start + split.getLength();
//		final Path file = split.getPath();
//		
////		compressionCodecs = new CompressionCodecFactory(job);
////		final CompressionCodec codec = compressionCodecs.getCodec(file);
//
//		// open the file and seek to the start of the split
//		FSDataInputStream fileIn = fs.open(split.getPath());
//		
//		boolean skipFirstLine = false;
//		
//		InputStream in = new OpenJDK7GZIPInputStream(fileIn);
//		
//		if (start != 0) {
//			skipFirstLine = true;
//			--start;
//			fileIn.seek(start);
//		}
		
	    String delimiter = job.get("textinputformat.record.delimiter");
	    byte[] recordDelimiter = null;
	    if (null != delimiter) {
	    	recordDelimiter = delimiter.getBytes();
	    }
		
//		if (skipFirstLine) { // skip first line and re-establish "start".
//			LineRecordReader.LineReader reader = new LineReader(in, job, recordDelimiter);
//			start += reader.readLine(new Text(), 0,
//					(int) Math.min((long) Integer.MAX_VALUE, end - start));
//		}		
		
		return new LineRecordReader(in, 0, Long.MAX_VALUE, job, recordDelimiter);

	}
}
