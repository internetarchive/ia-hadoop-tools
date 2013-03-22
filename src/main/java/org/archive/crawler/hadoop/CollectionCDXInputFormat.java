/**
 * 
 */
package org.archive.crawler.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * @author kenji
 *
 */
public class CollectionCDXInputFormat extends InputFormat<Integer, String> {
  private String collection;
  private List<InputSplit> splits;
  
  public CollectionCDXInputFormat(String collection) {
    this.collection = collection;
    this.splits = new ArrayList<InputSplit>();
  }
  
  public List<String> listFiles(String itemid) throws IOException {
    return null;
  }
  
  public List<String> listItems() throws IOException {
    int page = 1;
    int maxpage = Integer.MAX_VALUE;
    final String[] fields = new String[] { "identifier" };
    while (page <= maxpage) {
      String url = buildSearchURL(fields, page);
      
      HttpClient client = new DefaultHttpClient();
      HttpGet get = new HttpGet(url.toString());
      HttpResponse resp = client.execute(get);
      int code = resp.getStatusLine().getStatusCode();
      HttpEntity entity = resp.getEntity();
      InputStream is = entity.getContent();
      // TODO: parse XML response, add InputSplit to splits for each CDX file.
    }
    return new ArrayList<String>();
  }
  
  private String buildSearchURL(String[] fields, int page) {
    StringBuilder url = new StringBuilder("http://archive.org/advancedsearch.php");
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("q", "collection:" + collection);
    params.put("fl[]", fields);
    params.put("sort[]", new String[] { "publicdate asc" });
    params.put("indent", "");
    params.put("start", "0");
    params.put("page", Integer.toString(page));
    params.put("rows", 50);
    params.put("output", "xml");
    
    try {
      String psep = "?";
      for (Entry<String, Object> ent : params.entrySet()) {
	String k = ent.getKey();
	Object v = ent.getValue();
	if (v instanceof String[]) {
	  for (String p : (String[])v) {
	    url.append(psep).append(k).append("=")
	    .append(URLEncoder.encode(p, "UTF-8"));
	    psep = "&";
	  }
	} else {
	  url.append(psep).append(k).append("=")
	  .append(URLEncoder.encode(v.toString(), "UTF-8"));
	  psep = "&";
	}
      }
    } catch (UnsupportedEncodingException ex) {
    }
    return url.toString();
  }

  @Override
  public RecordReader<Integer, String> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    
    // TODO Auto-generated method stub
    return null;
  }

  public static class CDXFileSplit extends InputSplit {

    @Override
    public long getLength() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }
  }
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return splits;
  }

}
