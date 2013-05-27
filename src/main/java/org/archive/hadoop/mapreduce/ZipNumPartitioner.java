package org.archive.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import org.archive.util.binsearch.SortedTextFile;
import org.archive.util.iterator.CloseableIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONTokener;

public class ZipNumPartitioner<K, V> extends Partitioner<K, V> implements Configurable {
	
	public final static String ZIPNUM_PARTITIONER_CLUSTER = "conf.zipnum.partitioner.clusterSummary";
	public final static String ZIPNUM_PARTITIONER_JSON = "conf.zipnum.partitioner.jsonSplits";
	
	protected SortedTextFile summary = null;
	protected String splitsFile = null;
	
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
		
		if (numSplits <= 1) {
			return 0;
		}
		
		if (splitList == null) {
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
			if (summary == null) {
				return;
			}
			
			splitIter = summary.getSplitIterator(EMPTY_STRING, EMPTY_STRING, numSplits);
			
			splitList = new ArrayList<String>();
			
			// Skip first line, don't need the beginning line here
			if (splitIter.hasNext()) {
				splitIter.next();
			}			
			
			while (splitIter.hasNext()) {
				String str = splitIter.next();
				int keyEndIndex = str.indexOf(' ');
				if (keyEndIndex >= 0) {
					str = str.substring(0, keyEndIndex);
				}
				splitList.add(str);
				//System.out.println(str);
			}
			
			if ((numSplits - 1) != splitList.size()) {
				throw new RuntimeException("splitList size != " + (numSplits - 1));
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
		
		if (clusterSummary != null) {		
			try {
				summary = new SortedTextFile(clusterSummary);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		
		splitsFile = conf.get(ZIPNUM_PARTITIONER_JSON);
		
		if (splitsFile != null) {
			try {
				loadJsonSplits(splitsFile, conf);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	protected void loadJsonSplits(String splitsFile, Configuration conf) throws IOException, JSONException
	{
		Path splitsPath = new Path(splitsFile);
		FileSystem fs = splitsPath.getFileSystem(conf);
		
		FSDataInputStream inputStream = fs.open(splitsPath);
		
		JSONTokener tokener = new JSONTokener(new InputStreamReader(inputStream));
		JSONArray root = new JSONArray(tokener);
		
		// 0th object is number of lines, actual split points are 1th index in the root array
		JSONArray splitsArray = root.getJSONArray(1);
		
		// Assuming the first and last values of the array are empty lines
		splitList = new ArrayList<String>(splitsArray.length() - 2);
		
		for (int i = 1; i < splitsArray.length() - 1; i++) {
			String split = splitsArray.getString(i);
			splitList.add(split);
			System.out.println(split);
		}
		
		inputStream.close();
	}
}
