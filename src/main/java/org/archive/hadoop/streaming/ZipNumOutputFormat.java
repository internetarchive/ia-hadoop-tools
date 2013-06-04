package org.archive.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.util.Progressable;
import org.archive.hadoop.util.PartitionName;

public class ZipNumOutputFormat extends FileOutputFormat<Text, Text> {
	private int count;
	private static final int DEFAULT_ZIP_NUM_LINES = 3000;
	private static final String ZIP_NUM_LINES_CONFIGURATION = "conf.zipnum.count";
	private static final String ZIP_NUM_OVERCRAWL_CONFIGURATION = "conf.zipnum.overcrawl.daycount";
	
	private static final String ZIP_NUM_PART_MOD = "conf.zipnum.partmod";
	private static final String ZIP_NUM_CDX_HEADER = "conf.zipnum.cdxheader";	
	private static final String DEFAULT_PART_MOD = "a-";
	private String partMod = "";

	public ZipNumOutputFormat() {
		this(DEFAULT_ZIP_NUM_LINES);
	}

	public ZipNumOutputFormat(int count) {
		this.count = count;
	}

	public static void setZipNumLineCount(Configuration conf, int count) {
		conf.setInt(ZIP_NUM_LINES_CONFIGURATION, count);
	}

	public static void setZipNumOvercrawlDayCount(Configuration conf, int count) {
		conf.setInt(ZIP_NUM_OVERCRAWL_CONFIGURATION, count);
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
			JobConf conf, String name, Progressable progress) throws IOException {


		//Configuration conf = job.getConfiguration();
		count = conf.getInt(ZIP_NUM_LINES_CONFIGURATION, DEFAULT_ZIP_NUM_LINES);
		//int dayLimit = conf.getInt(ZIP_NUM_OVERCRAWL_CONFIGURATION, -1);
		
		partMod = conf.get(ZIP_NUM_PART_MOD, DEFAULT_PART_MOD);

		String partitionName = getPartitionName(conf);
		Path mainFile = getWorkFile(conf, partitionName + ".gz");
		Path summaryFile = getWorkFile(conf, partitionName + "-idx");
		

		FileSystem mainFs = mainFile.getFileSystem(conf);
		FileSystem summaryFs = summaryFile.getFileSystem(conf);
		
		int buffSize = conf.getInt("io.file.buffer.size", 4096);
		FSDataOutputStream mainOut = mainFs.create(mainFile, false, buffSize, progress);
		FSDataOutputStream summaryOut = summaryFs.create(summaryFile, false, buffSize, progress);
		
		String cdxHeader = conf.get(ZIP_NUM_CDX_HEADER);
		
		return new ZipNumRecordWriter(count, mainOut, summaryOut, partitionName, cdxHeader);
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
	
	public String getPartitionName(JobConf conf)
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
