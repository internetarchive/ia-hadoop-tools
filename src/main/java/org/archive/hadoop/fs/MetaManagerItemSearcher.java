package org.archive.hadoop.fs;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.mortbay.util.ajax.JSON;

/**
 * Searches items in given collection with MetaManager (metamgr.php).
 * MetaManager is restricted to authenticated users, can lookup all items with complex
 * query, but its JSON API has critical issues that makes it almost useless for item lookup.
 * this code is here just in case new service similar to current MetaManager replaces it. 
 * @author Kenji Nagahashi
 *
 */
public class MetaManagerItemSearcher implements ItemSearcher {
  private static Log LOG = LogFactory.getLog(MetaManagerItemSearcher.class);

  protected PetaboxFileSystem fs;
  protected URI fsUri;
  protected int maxRetries = 10;
  protected int retryDelay = 2000; // milliseconds
  protected int connectionTimeout = 60*1000;
  protected int socketTimeout = 0; // milliseconds, 0=infinite

  public final static int SEARCH_ROWS_PER_PAGE = 200;

  public MetaManagerItemSearcher() {
  }
  
  @Override
  public void initialize(PetaboxFileSystem fs, URI fsUri, Configuration conf) {
    this.fs = fs;
    this.fsUri = fsUri;
    String confbase = "fs." + fsUri.getScheme();
    
    maxRetries = conf.getInt(confbase + ".max-retries", 10);
  }
    
  protected static long sqldatetime2timestamp(String sqldatetime) {
    if (sqldatetime == null) return 0;
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    try {
      Date date = df.parse(sqldatetime);
      return date.getTime();
    } catch (ParseException ex) {
      return 0;
    }
  }
  
  protected static boolean inCollection(String iid, String collections) {
    int s = collections.indexOf(iid);
    if (s < 0) return false;
    int e = s + iid.length();
    if (s == 0) {
      return e >= collections.length() || collections.charAt(e) == ';';
    } else {
      return collections.charAt(s - 1) == ';' && 
      (e >= collections.length() || collections.charAt(e) == ';');
    }
  }

  public final static int METAMGR_ROWS_PER_PAGE = 200;

  protected URI buildMetaManagerURI(String itemid, int start) throws URISyntaxException {
    StringBuilder params = new StringBuilder();
    params.append("srt=identifier");
    params.append("&ord=asc");
    params.append("&w_collection=*").append(itemid).append("*");
    params.append("&fs_identifier=on&fs_mediatype=on&fs_collection=on");
    params.append("&off=").append(Integer.toString(start));
    // getting all often results in 504 error for big collection.
    //params.append("&lim=0"); // "all"
    params.append("&lim=").append(Integer.toString(METAMGR_ROWS_PER_PAGE));
    params.append("&output_format=json");
    //return new URI("http", fsUri.getAuthority(), "/metamgr.php", params.toString(), null);
    return new URI("http", "www.us.archive.org", "/metamgr.php", params.toString(), null);
  }
  
  
  /* (non-Javadoc)
   * @see org.archive.crawler.hadoop.ItemSearcher#searchItems(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public FileStatus[] searchItems(String itemid) throws IOException {
    LOG.info("looking up items in collection " + itemid + " with metamgr");
    List<FileStatus> result = new ArrayList<FileStatus>();
    int start = 0;
    // total number of results is not available in metamgr's JSON response. 
    long numresults = Long.MAX_VALUE;
    while (start < numresults) {
      URI uri;
      try {
	uri = buildMetaManagerURI(itemid, start);
	LOG.info("search uri=" + uri);
      } catch (URISyntaxException ex) {
	throw new IOException("failed to build URI for itemid=" + itemid + ", start=" + start, ex);
      }
      HttpGet get = fs.createHttpGet(uri);
      HttpEntity entity = null;
      Map<String, Object> jo = null;
      int retries = 0;
      do {
	if (retries > 0) {
	  try {
	    Thread.sleep(retryDelay);
	  } catch (InterruptedException ex) {
	  }
	}
	HttpResponse resp;
	try {
	  resp = fs.getHttpClient().execute(get);
	} catch (IOException ex) {
	  LOG.warn("connection to " + uri + " failed", ex);
	  if (++retries > maxRetries) {
	    throw new IOException(uri + ": retry exhausted trying to connect");
	  }
	  continue;
	}
	StatusLine st = resp.getStatusLine();
	entity = resp.getEntity();
	switch (st.getStatusCode()) {
	case 200:
	  if (retries > 0) {
	    LOG.info(uri + ": succeeded after " + retries + " retry(ies)");
	  }
	  // it appears search engine often fails to return JSON formatted output despite
	  // status code 200. detect it here.
	  Reader reader = new InputStreamReader(entity.getContent(), "UTF-8");
	  try {
	    jo = (Map<String, Object>)JSON.parse(reader);
	  } catch (IllegalStateException ex) {
	    LOG.error("JSON.parse failed", ex);
	    StringWriter w = new StringWriter();
	    int c;
	    while ((c = reader.read()) != -1) {
	      w.write(c);
	    }
	    LOG.error("rest of response:" + w.toString());
	  }
	  reader.close();
	  if (jo == null) {
	    LOG.warn(uri + " returned 200, but JSON parser failed on entity");
	    if (++retries > maxRetries) {
	      throw new IOException(uri + ": retry exhausted on " + uri);
	    }
	    continue;
	  }
	  break;
	case 502:
	case 503:
	case 504:
	  if (entity != null)
	    entity.getContent().close();
	  if (++retries > maxRetries) {
	    throw new IOException(uri + ": retry exhausted on "
		+ st.getStatusCode() + " " + st.getReasonPhrase());
	  }
	  LOG.warn(uri + " failed " + st.getStatusCode() + " "
	      + st.getReasonPhrase() + ", retry " + retries);
	  entity = null;
	  continue;
	default:
	  entity.getContent().close();
	  throw new IOException(uri + ": " + st.getStatusCode() + " " + st.getReasonPhrase());
	}
      } while (jo == null);
      // fields are returned in an array. we assume they are always in the same
      // order as fs_* parameters appears in query URL.
      // TODO: we could at least put a check of field names here.
      Object[] rows = (Object[])jo.get("rows");
      if (rows == null) {
	break; //?
      }
      for (int i = 0; i < rows.length; i++) {
	Object[] row = (Object[])rows[i];
	if (row == null) continue; // just in case...
	String iid = (String)row[0];
	if (iid == null) continue;
	// exclude collection items
	String mediatype = (String)row[1];
	if ("collection".equals(mediatype)) continue;
	// collection query pattern is not specific enough. check whether item
	// really have itemid as its collection.
	if (!inCollection(itemid, (String)row[2])) continue;
	
	String publicdate = row.length > 3 ? (String)row[3] : null;
	long mtime = sqldatetime2timestamp(publicdate);
	Path qf = new Path(fsUri.toString(), "/" + iid);
	LOG.debug("collection:" + itemid + " qf=" + qf);
	FileStatus fst = new FileStatus(0, true, 2, 4096, mtime, qf);
	result.add(fst);
      }
      start += rows.length;
    }
    LOG.info(String.format("searchItems(collection=%s): returning %d items", itemid, result.size()));
    return result.toArray(new FileStatus[result.size()]);
  }

}
