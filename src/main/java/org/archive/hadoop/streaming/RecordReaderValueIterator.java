package org.archive.hadoop.streaming;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.archive.util.iterator.CloseableIterator;

class RecordReaderValueIterator implements CloseableIterator<String>
{
	protected RecordReader<LongWritable, Text> recordReader;
	protected LongWritable key;
	protected Text value;
	
	public RecordReaderValueIterator(RecordReader<LongWritable, Text> recordReader)
	{
		this.recordReader = recordReader;
		key = this.recordReader.createKey();
		value = this.recordReader.createValue();
	}
	
	public boolean hasNext() {
		try {
			if (recordReader.next(key, value)) {
				return true;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return false;
	}

	public String next() {
		try {
			if (value != null) {
				return value.toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	public void remove() {
		
	}
	
	public void close()
	{
					
	}
}