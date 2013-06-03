package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.archive.hadoop.util.PartitionName;

public class NativeZipNumOutputFormat extends FileOutputFormat<Text, Text> {

	

	public static void setZipNumLineCount(Configuration conf, int count) {
		org.archive.hadoop.mapreduce.ZipNumOutputFormat.setZipNumLineCount(conf, count);
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
			JobConf conf, String name, Progressable progress) throws IOException {
		
		int count = org.archive.hadoop.mapreduce.ZipNumOutputFormat.getZipNumLineCount(conf);
		String partMod = org.archive.hadoop.mapreduce.ZipNumOutputFormat.getPartMod(conf);

		String partitionName = getPartitionName(conf, partMod);
	    // Obtain the compression codec from the Hadoop environment.
	    Class<? extends CompressionCodec> codecClass = getOutputCompressorClass( conf, GzipCodec.class );
	    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance( codecClass, conf );
	    // System.err.println( "Using codec:" + codec.toString() );

	    // Use a file extension basd on the codec, don't hard-code it.
	    Path mainFile = getWorkFile(conf, partitionName + codec.getDefaultExtension() );
		Path summaryFile = getWorkFile(conf, partitionName + "-idx");
		

		FileSystem mainFs = mainFile.getFileSystem(conf);
		FileSystem summaryFs = summaryFile.getFileSystem(conf);
		
		int buffSize = conf.getInt("io.file.buffer.size", 4096);
		FSDataOutputStream mainOut = mainFs.create(mainFile, false, buffSize, progress);
		FSDataOutputStream summaryOut = summaryFs.create(summaryFile, false, buffSize, progress);
		
		//return new ZipNumRecordWriter(count, mainOut, summaryOut, partitionName);
	    return new org.archive.hadoop.streaming.NativeZipNumRecordWriter( codec, mainOut, summaryOut, partitionName, count );
	}

	/**
	 * Get the path and filename for the output format.
	 * 
	 * @param conf
	 *            the task context
	 * @param extension
	 *            an extension to add to the filename
	 * @return a full path $output/_temporary/$taskid/part-[mr]-$id
	 * @throws IOException
	 */
	public Path getWorkFile(JobConf conf, String partWithExt)
			throws IOException {
		//FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		//return new Path(this.getWorkOutputPath(context.getJobConf()), partWithExt);
		return FileOutputFormat.getTaskOutputPath(conf, partWithExt);
	}
	
	public String getPartitionName(JobConf conf, String partMod)
	{
		//TaskID taskId = conf.getTaskAttemptID().getTaskID();
		TaskID taskId = TaskAttemptID.forName(conf.get("mapred.task.id")).getTaskID();
		int partition = taskId.getId();
		String basename = 
			PartitionName
				.getPartitionOutputName(conf, partition);
		if (basename == null) {
			// use default name:
			basename = String.format("part-%s%05d", partMod, partition);
		}
		
		return basename;
	}
}
