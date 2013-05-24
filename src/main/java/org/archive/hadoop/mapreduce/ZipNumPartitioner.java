package org.archive.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.archive.util.binsearch.SortedTextFile;
import org.archive.util.iterator.CloseableIterator;

public class ZipNumPartitioner<K, V> extends Partitioner<K, V> implements Configurable {
	
	public final static String ZIPNUM_PARTITIONER_CLUSTER = "pig.zipnum.partitioner.clusterSummary";
	
	protected SortedTextFile summary = null;
	
	public ZipNumPartitioner()
	{
		
	}
	
	protected final static String EMPTY_STRING = "";
	
	protected List<String> splitList = null;
	
	@Override
	public int getPartition(K key, V value, int numSplits) {
		return getPartition(key.toString(), numSplits);
	}
	
	public int getPartition(String searchKey, int numSplits) {
		
		if (summary == null) {
			return 0;
		}
		
		if (numSplits <= 1) {
			return 0;
		}
		
		if ((splitList == null) || (splitList.size() != (numSplits - 1))) {
			loadSplits(numSplits);
		}
				
		int spaceIndex = searchKey.indexOf(' ');
		
		if (spaceIndex >= 0) {
			searchKey = searchKey.substring(0, spaceIndex);
		}
		
		int index = linSearchSplits(searchKey);
		//index = (int)(Math.random() * 5);
		return index;
	}
	
	protected int linSearchSplits(String key)
	{
		int index = 0;
		
		for (String split : splitList) {
			if (key.compareTo(split) <= 0) {
				return index;
			}
			index++;
		}
		
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
