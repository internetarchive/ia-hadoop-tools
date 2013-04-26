package org.archive.hadoop.fs;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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
import org.archive.petabox.PetaboxClient;
import org.mortbay.util.ajax.JSON;

/**
 * Searches items in given collection with IA's search engine.
 * search engine is publicly accessible, but can only find items indexed by search engine.
 * those items marked "NoIndex" will not be returned by this implementation. 
 * @author kenji
 *
 */
public class SearchEngineItemSearcher implements ItemSearcher {
  private static Log LOG = LogFactory.getLog(SearchEngineItemSearcher.class);

  protected PetaboxFileSystem fs;
  protected URI fsUri;
  protected int maxRetries = 10;
  protected int retryDelay = 2000; // milliseconds
  protected int connectionTimeout = 60*1000;
  protected int socketTimeout = 0; // milliseconds, 0=infinite

  public final static int SEARCH_ROWS_PER_PAGE = 200;

  public SearchEngineItemSearcher() {
  }
  
  @Override
  public void initialize(PetaboxFileSystem fs, URI fsUri, Configuration conf) {
    this.fs = fs;
    this.fsUri = fsUri;
    String confbase = "fs." + fsUri.getScheme();
    
    maxRetries = conf.getInt(confbase + ".max-retries", 10);
  }
  
  protected static long isodatetime2timestamp(String isodatetime) {
    if (isodatetime == null) return 0;
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
    try {
      Date date = df.parse(isodatetime);
      return date.getTime();
    } catch (ParseException ex) {
      return 0;
    }
  }
  
  protected URI buildSearchURI(String itemid, int start) throws URISyntaxException {
    StringBuilder params = new StringBuilder();
    params.append("q=collection:").append(itemid);
    params.append("&fl[]=identifier&fl[]=publicdate");
    params.append("&sort[]=publicdate+asc");
    params.append("&indent=&start=").append(start);
    params.append("&rows=").append(SEARCH_ROWS_PER_PAGE);
    params.append("&output=json");
    return new URI("http", fsUri.getAuthority(), "/advancedsearch.php", params.toString(), null);
  }
  
  /* (non-Javadoc)
   * @see org.archive.crawler.hadoop.ItemSearcher#searchItems(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public FileStatus[] searchItems(String itemid) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    int start = 0;
    long numresults = Long.MAX_VALUE;
    while (start < numresults) {
      URI uri;
      try {
	uri = buildSearchURI(itemid, start);
	LOG.debug("search uri=" + uri);
      } catch (URISyntaxException ex) {
	throw new IOException("failed to build URI for itemid=" + itemid + ", start=" + start, ex);
      }
      PetaboxClient pbclient = fs.getPetaboxClient();
//      HttpClient client = fs.getHttpClient();
//      HttpGet get = fs.createHttpGet(uri);
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
	  //resp = client.execute(get);
		resp = pbclient.doGet(uri);
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
	  try {
	    Reader reader = new InputStreamReader(entity.getContent(), "UTF-8");
	    jo = (Map<String, Object>)JSON.parse(reader);
	    reader.close();
	  } catch (IOException ex) {
	    LOG.warn(uri + " error reading 200 response: " + ex.getMessage());
	    if (++retries > maxRetries) {
	      throw new IOException(uri + ": retry exhausted");
	    }
	    continue;
	  }
	  if (jo == null) {
	    LOG.warn(uri + " returned 200, but JSON parser failed on entity");
	    if (++retries > maxRetries) {
	      throw new IOException(uri + ": retry exhausted");
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
	  throw new IOException(st.getStatusCode() + " " + st.getReasonPhrase());
	}
      } while (jo == null);
      Map<String, Object> jresp = (Map<String, Object>)jo.get("response");
      // is this a failure scenario that should be retried?
      if (jresp == null) break;
      Long numfound = (Long)jresp.get("numFound");
      numresults = numfound != null ? numfound : 0;
      Object[] jdocs = (Object[])jresp.get("docs");
      // TODO: log warning?
      if (jdocs == null || jdocs.length == 0) break;
      for (int i = 0; i < jdocs.length; i++) {
	Map<String, Object> jdoc = (Map<String, Object>)jdocs[i];
	if (jdoc != null) {
	  String iid = (String)jdoc.get("identifier");
	  if (iid == null) continue;
	  String publicdate = (String)jdoc.get("publicdate"); // ISO format
	  long mtime = isodatetime2timestamp(publicdate);
	  Path qf = new Path(fsUri.toString(), "/" + iid);
	  LOG.debug("collection:" + itemid + " qf=" + qf);
	  FileStatus fst = new FileStatus(0, true, 2, 4096, mtime, qf);
	  result.add(fst);
	}
      }
      start += jdocs.length;
    }
    LOG.info(String.format("searchItems(collection=%s): returning %d items", itemid, result.size()));
    return result.toArray(new FileStatus[result.size()]);
  }

}
