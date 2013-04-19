package org.archive.hadoop.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.archive.util.binsearch.SortedTextFile;
import org.archive.util.iterator.CloseableIterator;

public class ZipNumPartitioner extends Partitioner<Text, Text> implements Configurable {
	
	public final static String ZIPNUM_PARTITIONER_CLUSTER = "pig.zipnum.partitioner.clusterSummary";
	
	//protected ZipNumCluster cluster;
	SortedTextFile summary = null;
	
	public ZipNumPartitioner()
	{
		
	}
	
	protected final static String EMPTY_STRING = "";
	
	protected List<String> splitList = null;
	
	@Override
	public int getPartition(Text key, Text value, int numSplits) {
		
		if (summary == null) {
			return 0;
		}
		
		if ((splitList == null) || (splitList.size() != numSplits)) {
			loadSplits(numSplits);
		}
		
		String valueURL = value.toString();
		//String valueURL = (String)key.getValueAsPigType();
		//String valueURL = (key.isNull() ? value.toString() : key.toString());
		
		int spaceIndex = valueURL.indexOf(' ');
		
		if (spaceIndex >= 0) {
			valueURL = valueURL.substring(0, spaceIndex);
		}
		
//		key.setIndex((byte)0);
//		value.setIndex((byte)0);
		
		int index = binSearchSplits(valueURL);
		return index;
	}

	protected int binSearchSplits(String key)
	{
		int loc = Collections.binarySearch(splitList, key);
		if (loc < 0) {
			loc = (loc * -1) - 2;
			if (loc < 0) {
				loc = 0;
			}
		}
		
		return loc;
	}
	
	protected void loadSplits(int numSplits)
	{
		CloseableIterator<String> splitIter = null;
		
		try {
			splitIter = summary.getSplitIterator(EMPTY_STRING, EMPTY_STRING, numSplits);
			
			splitList = new ArrayList<String>();
			
			while (splitIter.hasNext()) {
				String str = splitIter.next();
				int keyEndIndex = str.indexOf(' ');
				if (keyEndIndex >= 0) {
					str = str.substring(0, keyEndIndex);
				}
				splitList.add(str);
				System.out.println(str);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (splitIter != null) {
				try {
					splitIter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setConf(Configuration conf) {
		String clusterSummary = conf.get(ZIPNUM_PARTITIONER_CLUSTER);
		
		try {
			summary = new SortedTextFile(clusterSummary);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
