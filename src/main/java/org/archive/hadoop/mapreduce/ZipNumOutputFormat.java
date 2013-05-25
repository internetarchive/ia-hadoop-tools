package org.archive.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.archive.hadoop.util.PartitionName;

/**
 * Custom TextOutputFormat which produces output in the "zipnum"
 * format.  This is fairly specific to the needs of the Wayback
 * Machine CDX "clusters".
 * <p>
 * Most of the heavy lifting is done by
 * <code>ZipNumRecordWriter</code>, this class just sets-up the
 * environment:
 * <ul>
 * <li>instantiate the Hadoop codec to compress the output</li>
 * <li>create the output file</li>
 * <li>create summary file</li>
 * </ul>
 */
public class ZipNumOutputFormat extends TextOutputFormat<Text, Text> 
{
  public static final int    DEFAULT_ZIP_NUM_LINES = 3000;
  public static final String ZIP_NUM_LINES_CONFIGURATION = "conf.zipnum.count";
  
  public static final String ZIP_NUM_PART_MOD = "conf.zipnum.partmod";
  public static final String DEFAULT_PART_MOD = "a-";

  public String partMod = "";

  
  /**
   * Construct a <code>ZipNumOutputFormat</code> with the default number of lines per compressed envelope.
   */
  public ZipNumOutputFormat( )
  {
	  
  }
  
  /**
   * Set the number of lines per compressed envelope.
   */
  public static void setZipNumLineCount( Configuration conf, int count ) 
  {
    conf.setInt( ZIP_NUM_LINES_CONFIGURATION, count );
  }
  
  public static int getZipNumLineCount( Configuration conf ) {
    return conf.getInt(ZIP_NUM_LINES_CONFIGURATION, DEFAULT_ZIP_NUM_LINES);
  }
  
  public static String getPartMod( Configuration conf ) {
    return conf.get( ZIP_NUM_PART_MOD, DEFAULT_PART_MOD );
  }
  
  /**
   *
   */
  @Override
  public RecordWriter<Text, Text> getRecordWriter( TaskAttemptContext context ) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();

    int count = getZipNumLineCount(conf);
    
    partMod = getPartMod(conf);
    String partitionName = getPartitionName( context );
    
    // Obtain the compression codec from the Hadoop environment.
    Class<? extends CompressionCodec> codecClass = getOutputCompressorClass( context, GzipCodec.class );
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance( codecClass, conf );
    // System.err.println( "Using codec:" + codec.toString() );

    // Use a file extension basd on the codec, don't hard-code it.
    Path mainFile = getWorkFile(context, partitionName + codec.getDefaultExtension() );
    Path summaryFile = getWorkFile(context, partitionName + "-idx");

    FileSystem mainFs = mainFile.getFileSystem(conf);
    FileSystem summaryFs = summaryFile.getFileSystem(conf);

    FSDataOutputStream mainOut = mainFs.create(mainFile, false);
    FSDataOutputStream summaryOut = summaryFs.create(summaryFile, false);
    
    return new ZipNumRecordWriter( codec, mainOut, summaryOut, partitionName, count );
  }
  
  /**
   * Get the path and filename for the output format.
   */
  public Path getWorkFile( TaskAttemptContext context, String partWithExt ) throws IOException 
  {
    FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
    return new Path(committer.getWorkPath(), partWithExt);
  }
	
  /**
   * Retrieve partition name based on the current task ID and a custom "partMod".
   */
  public String getPartitionName( TaskAttemptContext context )
  {
    TaskID taskId = context.getTaskAttemptID().getTaskID();  
    int partition = taskId.getId();
    String basename = PartitionName.getPartitionOutputName(context.getConfiguration(), partition);
    if( basename == null ) 
      {
        // use default name:
        basename = String.format("part-%s%05d", partMod, partition);
      }
    
    return basename;
  }

}
