package org.archive.cassandra;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class CassCDXOutputFormat extends OutputFormat<Text, Text>
{

	@Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
		
		return new CassCDXRecordWriter(context.getConfiguration());
    }

	@Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
	    // TODO Auto-generated method stub
	    
    }

	@Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        
		return new NullOutputCommitter();
    }
	
	public static class NullOutputCommitter extends OutputCommitter {
		  public void abortTask(TaskAttemptContext taskContext) { }

		  public void cleanupJob(JobContext jobContext) { }

		  public void commitTask(TaskAttemptContext taskContext) { }

		  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
		    return false;
		  }

		  public void setupJob(JobContext jobContext) { }

		  public void setupTask(TaskAttemptContext taskContext) { }
		}
	
}