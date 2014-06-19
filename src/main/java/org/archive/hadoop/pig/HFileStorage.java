/**
 * 
 */
package org.archive.hadoop.pig;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.mortbay.util.ajax.JSON;

/**
 * {@link StoreFunc} for writing tuples into HFile.
 * Tuple should have three members:
 * <div>0: row key</div>
 * <div>1: map with primitive values</div>
 * <div>2: timestamp</div>
 * map will be converted into JSON and stored in single column whose name is specified
 * as constructor argument to HFileStorage.
 * 
 * Note: this class is not yet fully tested against CDH5 (Pig-0.12 / HBase-0.96.1.1)
 *
 * @author Kenji Nagahashi
 *
 */
public class HFileStorage extends StoreFunc {
  private static final Log LOG = LogFactory.getLog(HFileStorage.class);
  
  private String tableName;
  private byte[] columnFamily;
  private byte[] columnQualifier;
  
  private RecordWriter<ImmutableBytesWritable, KeyValue> writer;
  
  private byte[] lastRowkey;
  private long lastTimestamp;
  /**
   * 
   * @param tableName HTable name
   * @param columnName column name, either "FAMILY:COLUMNQUALIFIER" or "FAMILY".
   * @exception IllegalArgumentException 
   *	when FAMILY is empty (i.e. columnName starts with ":")
   */
  public HFileStorage(String tableName, String columnName) {
    this.tableName = tableName;
    int p = columnName.indexOf(':');
    if (p == -1) {
      this.columnFamily = Bytes.toBytes(columnName);
      this.columnQualifier = HConstants.EMPTY_BYTE_ARRAY;
    } else {
      this.columnFamily = Bytes.toBytes(columnName.substring(0, p));
      this.columnQualifier = Bytes.toBytes(columnName.substring(p + 1));
      if (columnFamily.length == 0)
	throw new IllegalArgumentException("empty column family");
    }
    this.lastRowkey = null;
    this.lastTimestamp = 0;
  }
  /* (non-Javadoc)
   * @see org.apache.pig.StoreFunc#getOutputFormat()
   */
  @Override
  public OutputFormat<?,?> getOutputFormat() throws IOException {
    return new HFileOutputFormat();
  }

  /* (non-Javadoc)
   * @see org.apache.pig.StoreFunc#prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  /* (non-Javadoc)
   * @see org.apache.pig.StoreFunc#putNext(org.apache.pig.data.Tuple)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void putNext(Tuple t) throws IOException {
    if (t.size() < 2 || t.size() > 3)
      throw new IllegalArgumentException("wrong number of tuple members (must be 2 or 3)");
    String rowkey = t.get(0).toString();
    Map<String, Object> value = (Map<String, Object>)t.get(1);
    String json = value != null ? JSON.toString(value) : "";
    long ts = HConstants.LATEST_TIMESTAMP;
    if (t.size() > 2) {
      if (t.getType(2) != DataType.UNKNOWN) {
	ts = (Long)t.get(2);
      }
    }
    if (false) {
    // check key order (HFileOutputFormat's RecordWriter also does this check)
    byte[] browkey = Bytes.toBytes(rowkey);
    if (lastRowkey != null) {
      int comp = Bytes.compareTo(lastRowkey, browkey);
      if (comp > 0)
	throw new IOException("rowkey not in order:"
	    + Bytes.toStringBinary(lastRowkey) + ">"
	    + Bytes.toStringBinary(browkey));
      else if (comp == 0 && lastTimestamp >= ts)
	throw new IOException("timestamp not in order:" + lastTimestamp + ">="
	    + ts + " for rowkey " + Bytes.toStringBinary(browkey));
    }
    lastRowkey = browkey;
    lastTimestamp = ts;
    }
    
    KeyValue kv = new KeyValue(Bytes.toBytes(rowkey), columnFamily,
	columnQualifier, ts, Bytes.toBytes(json));
    // passing null to key, because write() makes no use of it.
    try {
      writer.write((ImmutableBytesWritable)null, kv);
    } catch (InterruptedException ex) {
      throw new IOException("interrupted");
    }
  }
  
  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    ResourceFieldSchema[] fields = s.getFields();
    if (fields.length < 2 || fields.length > 3)
      throw new IOException("input must be URL, Map[, timestamp]");
    if (fields[0].getType() != DataType.CHARARRAY)
      throw new IOException("first field URL must be a CHARARRAY");
    if (fields[1].getType() != DataType.MAP)
      throw new IOException("second field must be a MAP");
    if (fields.length == 3 && fields[2].getType() != DataType.LONG)
      throw new IOException("third field timestamp must be a LONG");
  }

  /* (non-Javadoc)
   * @see org.apache.pig.StoreFunc#setStoreLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    HFileOutputFormat.setOutputPath(job, new Path(location));
    if (tableName != null && !tableName.equals(""))
      configureIncrementalLoad(job);
  }
  /**
   * set up output partitioning appropriate for current region configuration.
   * this is mostly a copy of {@link HFileOutputFormat#configureIncrementalLoad(Job, HTable)},
   * which sets up not only partitioning, but also reducerClass, outputKeyClass, outputValueClass,
   * etc. - it is not suitable for use with PIG.
   * @param job Hadoop Job
   * @throws IOException
   */
  private void configureIncrementalLoad(Job job) throws IOException {
    // setup HFile partitioning appropriate for HTable's regions
	Configuration conf = job.getConfiguration();
    HTable table = new HTable(conf, tableName);
    // configureIncrementalLoad() sets up not only partitioning, but also reducerClass, outputKeyClass,
    // outputValueClass, etc. It is not appropriate for use with PIG.
    //HFileOutputFormat.configureIncrementalLoad(job, table);
    // following code is stolen from HFileOutputFormat
    job.setPartitionerClass(TotalOrderPartitioner.class);
    LOG.info("Looking up current regions for table " + tableName);
    List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
//    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
//	"to match current region count");
//    job.setNumReduceTasks(startKeys.size());
    Path partitionsPath = new Path(job.getWorkingDirectory(),
        "partitions_" + System.currentTimeMillis());
    LOG.info("Writing partition information to " + partitionsPath);

    FileSystem fs = partitionsPath.getFileSystem(conf);
    writePartitions(conf, partitionsPath, startKeys);
    partitionsPath.makeQualified(fs);
    URI cacheUri;
    try {
      cacheUri = new URI(partitionsPath.toString() + "#" +
          TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(cacheUri, conf);
    DistributedCache.createSymlink(conf);
    
    LOG.info("Incremental table output configured.");
  }

  /**
   * Return the start keys of all of the regions in this table,
   * as a list of ImmutableBytesWritable.
   */
  private static List<ImmutableBytesWritable> getRegionStartKeys(HTable table)
  throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    ArrayList<ImmutableBytesWritable> ret =
      new ArrayList<ImmutableBytesWritable>(byteKeys.length);
    for (byte[] byteKey : byteKeys) {
      ret.add(new ImmutableBytesWritable(byteKey));
    }
    return ret;
  }
  /**
   * Write out a SequenceFile that can be read by TotalOrderPartitioner
   * that contains the split points in startKeys.
   * @param partitionsPath output path for SequenceFile
   * @param startKeys the region start keys
   */
  private static void writePartitions(Configuration conf, Path partitionsPath,
      List<ImmutableBytesWritable> startKeys) throws IOException {
    if (startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0
    TreeSet<ImmutableBytesWritable> sorted =
      new TreeSet<ImmutableBytesWritable>(startKeys);

    ImmutableBytesWritable first = sorted.first();
    if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
      throw new IllegalArgumentException(
          "First region of table should have empty start key. Instead has: "
          + Bytes.toStringBinary(first.get()));
    }
    sorted.remove(first);
    
    // Write the actual file
    FileSystem fs = partitionsPath.getFileSystem(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
        conf, partitionsPath, ImmutableBytesWritable.class, NullWritable.class);
    
    try {
      for (ImmutableBytesWritable startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }
}
