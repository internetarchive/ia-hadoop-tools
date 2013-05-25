package org.archive.hadoop.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;

/**
 * RecordWriter which produces "zipnum" output format.  This is fairly
 * specific to the needs of the Wayback Machine CDX "clusters".
 * <p>
 * It only handles Text keys and values, and only outputs text records
 * in "zipnum" format.  The output text format is just the key and
 * value that are passed in, delimited by DELIMITER (' ').
 * <p>
 * For "zipnum" format, the output records are compressed with the
 * given Hadoop <code>codec</code>, and after <code>limit</code> lines
 * are written, the compression stream is closed and a new one is
 * open.  This gives us the "catenated compression envelopes" format
 * that is used frequently at Internet Archive.
 * <p>
 * Whenever a compression envelope is closed, a summary line is 
 * written to an <code>*-idx</code> file.  This summary/idx file
 * records the first key of the compression envelope and the
 * starting byte-offset of the envelope and the envelope size
 * in bytes, e.g.
 * <pre>
 *   org,example)	0	128
 * </pre>
 * The fields of the summary/idx file are delimited with tabs.
 * <p>
 * The trick to make this work is to use the
 * <code>NotClosingDataOutputStream</code> to trap the calls to
 * <code>close()</code> by the <code>codec</code>'s output stream.
 * <p>
 * When we close one compression envelope, we call the
 * <code>codec.flush()</code> and <code>codec.close()</code> methods
 * to ensure that the compressed output is flushed and the compression
 * footers are written to the underlying output stream.  But, the
 * codec will try to close the underlying output stream too, which we
 * want to prevent from happening because we want to start the next
 * compression envelope.  So, we trap the call to <code>close()</code>
 * and ignore it.  Then, create a new compression stream on top
 * of the existing underlying file output stream.
 */
public class ZipNumRecordWriter extends RecordWriter<Text, Text>
{
  // Since we are writing binary output, we just create some
  // <strong>int</strong> values for our literal characters we use
  // later.
  public static final int DELIMITER = ' ';
  public static final int NEWLINE   = '\n';
  public static final int SUMMARY_DELIMITER = '\t';

  public FSDataOutputStream out;
  public CompressionCodec codec;
  public DataOutputStream compressing;
  public FSDataOutputStream summary;
  public String partitionName;
   
  public Text startKey;
  public long oldPos = 0;
  public long count  = 0;
  public long limit  = 0;
  
  /**
   * Construct a ZipNumRecordWriter.
   */
  public ZipNumRecordWriter( CompressionCodec codec, FSDataOutputStream out, FSDataOutputStream summary, String partitionName, long limit ) throws IOException
  {
    this.limit = limit;
    
    this.out = out;

    this.codec = codec;
    this.compressing  = startCompressionStream( this.codec, this.out );
    
    this.summary = summary;
    this.partitionName = partitionName;
  }

  /**
   * Write the key,value pair to the compressed output stream.  Once we write <code>limit</code>
   * records, close the comrpression envelope and start another one; also write a summary line.
   */
  @Override
  public synchronized void write( Text key, Text value ) throws IOException
  {
    if ( count == 0 )
      {
        // NOTE: It's important to create a *new* Text here.  The
        //       'key' passed-in is modified by the caller.  So if we
        //       just keep a reference to the 'key', then those
        //       modifications will also apply to our 'startKey'.
        startKey = new Text( key );
      }
    
    // Write the output record to the compressing stream.
    compressing.write( key.getBytes(), 0, key.getLength() );
    compressing.write( DELIMITER );
    compressing.write( value.getBytes(), 0, value.getLength() );
    compressing.write( NEWLINE ); 
    
    count++;
    if ( count == limit )
      {
        // Flush and close the current compression block/envelope.  
        // The close() method is supposed to flush() first, but you never know...
        compressing.flush();
        compressing.close();
        
        writeSummary();
        
        // Save the position and start the next compression envelope.
        oldPos = out.getPos();
        count  = 0;
        
        compressing  = startCompressionStream( codec, out );
      }
  }
  
  /**
   * Close the compression envelope, write a summary line, then
   * finally close the underlying output stream.
   */
  @Override
  public synchronized void close(TaskAttemptContext context) throws IOException 
  {
    // I'm paranoid about flushing :)
    compressing.flush();
    compressing.close();

    // It's possible no records were written to this output partition,
    // in which case we don't have a startKey to write to the summary.
    if ( startKey != null )
      {
        writeSummary();
      }
    
    out.flush();
    out.close();
  }
  
  /**
   * Convenience method to start a new compression output stream,
   * ensuring the use of the NotClosingDataOutputStream so that we can
   * prevent the codec stream from closing the underlying file stream.
   */
  public DataOutputStream startCompressionStream( CompressionCodec codec, DataOutputStream out ) throws IOException
  {
    return new DataOutputStream( codec.createOutputStream( new NotClosingDataOutputStream( out ) ) );
  }

  /**
   * Convenience method to write out a summary line.
   */
  public void writeSummary( ) throws IOException
  {
    summary.write( startKey.getBytes(), 0, startKey.getLength() );
    summary.write( SUMMARY_DELIMITER );
    summary.write( partitionName.getBytes("UTF-8") );
    summary.write( SUMMARY_DELIMITER );
    summary.write( Long.toString( oldPos ).getBytes("UTF-8") );
    summary.write( SUMMARY_DELIMITER );
    summary.write( Long.toString( out.getPos() - oldPos ).getBytes("UTF-8") );
    summary.write( NEWLINE ); 
    summary.flush();
    summary.close();
  }
  
  /**
   * Simple wrapper around a DataOutputStream which traps calls to
   * close() and ignores them.
   *
   * This purpose of this class is to wrap the underlying file which
   * is written by the compression codec stream.  When we close the
   * compression stream, it tries to close the underlying file
   * stream, which we want to avoid.  We want to keep the underlying
   * file stream open so that we can write the next compressed
   * block/envelope.
   */
  public static class NotClosingDataOutputStream extends DataOutputStream
  {
    public NotClosingDataOutputStream( DataOutputStream out )
    {
      super(out);
    }
    
    public void close()
    {
      // Refuse to close!
    }
  }

}
