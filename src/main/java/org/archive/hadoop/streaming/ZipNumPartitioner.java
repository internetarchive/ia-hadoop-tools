package org.archive.hadoop.streaming;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class ZipNumPartitioner extends org.archive.hadoop.mapreduce.ZipNumPartitioner<Text, Text> implements org.apache.hadoop.mapred.Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numSplits) {
		return super.getPartition(key, value, numSplits);
	}

	@Override
	public void configure(JobConf conf) {
		super.setConf(conf);
	}
}
