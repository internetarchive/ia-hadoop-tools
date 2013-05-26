package org.archive.hadoop.pig;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.builtin.TextLoader;

public class PerMapTextLoader extends TextLoader {
	
	Path sourcePath;
	Job currJob;

	@Override
	public void setLocation(String location, Job job) throws IOException {
		currJob = job;
		super.setLocation(location, job);
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
    	sourcePath = ((FileSplit)split.getWrappedSplit()).getPath();
    	
    	if (currJob != null) {
    		currJob.getConfiguration().set("map.input.file", sourcePath.toString());
    	}
    	
    	super.prepareToRead(reader, split);
	}
	
    @Override
    public InputFormat getInputFormat() {
    	return new PigTextInputFormat()
    	{
			@Override
			protected boolean isSplitable(JobContext context, Path file) {
				context.getConfiguration().set("map.input.file", file.toString());
				return false;
			}
    	};
    }
}
