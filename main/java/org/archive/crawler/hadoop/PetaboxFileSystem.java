/**
 * 
 */
package org.archive.crawler.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.archive.crawler.petabox.CookieFilePetaboxCredentialProvider;
import org.archive.crawler.petabox.PetaboxCredentialProvider;
import org.mortbay.util.ajax.JSON;

/**
 * PetaboxFileSystem allows Hadoop MapReduce to read data directly out of Internet Archive's
 * Petabox storage.
 * Only read operations are supported.
 * @author Kenji Nagahashi
 *
 */
public class PetaboxFileSystem extends FileSystem {
  private static Log LOG = LogFactory.getLog(PetaboxFileSystem.class);
  protected URI fsUri;
  private Path cwd = new Path("/");
  
  // authentication tokens
  protected String user;
  protected String sig;
  
  protected int maxRetries = 10;
  protected int retryDelay = 2000; // milliseconds
  protected int connectionTimeout = 60*1000;
  protected int socketTimeout = 60*1000; // milliseconds, 0=infinite
  // socket parameters for metadata API
  protected int metadataConnectionTimeout = 10*1000; // milliseconds
  protected int metadataSocketTimeout = 5*1000; // milliseconds
  
  protected String urlTemplate = null;
  
  /**
   * if true, PetaboxFileSystem makes up empty item when Metadata API tells it's non-existent,
   * instead of throwing FileNotFoundException.
   */
  protected boolean ignoreMissingItems = false;
  
  protected HttpClient client;

  private String hadoopJobInfo;
  
  // metadata cache
  private LRUMap metadataCache = new LRUMap(200);

  public static final String VERSION = "0.0.2";
  public static final String USER_AGENT = PetaboxFileSystem.class.getName() + "/" + VERSION;
  
  private Class<? extends ItemSearcher> itemSearcherClass = SearchEngineItemSearcher.class;
  protected ItemSearcher itemSearcher;
  
  /**
   * if non-null and non-emtpy, {@link #listStatus(Path)} will only return
   * those files of specified media types (unless explicitly specified).
   */
  protected String[] fileTypes;
  
  public PetaboxFileSystem() {
  }
  
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    // name.path is not necessarily be "/". it typically is what's passed to LOAD (i.e.
    // may contain wild-cards.)
    this.fsUri = name;
    this.cwd = new Path("/");
    
    String confbase = "fs." + fsUri.getScheme();
    
    // user credential
    this.user = conf.get(confbase + ".user");
    this.sig = conf.get(confbase + ".sig");
    if (this.user == null) {
      getCredentials(conf);
    }
    
    // ClientConnectionManager properties can be configured by config properties. 
    ThreadSafeClientConnManager connman = new ThreadSafeClientConnManager();
    int maxPerRoute = conf.getInt(confbase + ".max-per-route",
	connman.getDefaultMaxPerRoute());
    int maxTotal = conf.getInt(confbase + ".max-total",
	connman.getMaxTotal());
    connman.setDefaultMaxPerRoute(maxPerRoute);
    connman.setMaxTotal(maxTotal);
    this.client = new DefaultHttpClient(connman);
    
    this.maxRetries = conf.getInt(confbase + ".max-retries", this.maxRetries);
    this.retryDelay = conf.getInt(confbase + ".retry-delay", this.retryDelay);
    this.connectionTimeout = conf.getInt(confbase + ".connection-timeout", this.connectionTimeout);
    this.socketTimeout = conf.getInt(confbase + ".socket-timeout", this.socketTimeout);
    this.metadataConnectionTimeout = conf.getInt(confbase
	+ ".metadata.connection-timeout", this.metadataConnectionTimeout);
    this.metadataSocketTimeout = conf.getInt(confbase
	+ ".metadata.socket-timeout", this.metadataSocketTimeout);

    this.urlTemplate = conf.get(confbase + ".url-template");
    
    // this is hdfs:// URI for job staging, which contains job tracker
    // host:port, user name and JobID. Somewhat ugly, but provides useful info.
    this.hadoopJobInfo = conf.get("mapreduce.job.dir", "-");
    
    // ensure dependencies get shipped
    HadoopUtil.addDependencyJars(conf, HttpClient.class, HttpEntity.class, LRUMap.class);
    
    String itemSearcherClassName = conf.get(confbase + ".itemsearcher");
    if (itemSearcherClassName != null) {
      try {
	this.itemSearcherClass = Class.forName(itemSearcherClassName).asSubclass(ItemSearcher.class);
      } catch (ClassNotFoundException ex) {
	throw new IOException(confbase + ".itemsearcher=" + itemSearcherClassName + ": undefined class");
      }
    }
    try {
      LOG.info("initializing ItemSearcher " + this.itemSearcherClass.getName());
      this.itemSearcher = this.itemSearcherClass.newInstance();
      this.itemSearcher.initialize(this, fsUri, conf);
    } catch (Exception ex) {
      throw new IOException(this.itemSearcherClass.getName() + ": instantiation failed");
    }
    
    String fileTypes = conf.get(confbase + ".file-types");
    if (fileTypes != null && !fileTypes.isEmpty()) {
      String[] a = fileTypes.split("(\\s*,)+\\s*");
      for (int i = 0; i < a.length; i++) {
	a[i] = a[i].trim();
      }
      this.fileTypes = a;
    }
    
    this.ignoreMissingItems = conf.getBoolean(confbase + ".ignore-missing-items", this.ignoreMissingItems);
    
    LOG.info("PetaboxFileSystem.initialize:fsUri=" + fsUri);
  }
  /**
   * read user credentials from ~/.iaauth file with IA cookies.
   */
  private void getCredentials(Configuration conf) {
    PetaboxCredentialProvider provider = new CookieFilePetaboxCredentialProvider();
    this.user = provider.getUser();
    if (this.user != null) {
      conf.set("fs."+fsUri.getScheme()+".user", this.user);
    }
    this.sig = provider.getSignature();
    if (this.sig != null) {
      conf.set("fs."+fsUri.getScheme()+".sig", this.sig);
    }
  }
  
  protected String getHadoopJobInfo() {
    return hadoopJobInfo;
  }
  protected static class ReadOnlyFileSystemException extends IOException {
    private static final long serialVersionUID = -6540111017708582671L;
    public ReadOnlyFileSystemException() {
      super("PetaboxFileSystem is read-only");
    }
  }
  final static long parseLong(Object o) {
    return parseLong(o != null ? o.toString() : null);
  }
  final static long parseLong(String o) {
    if (o == null || o.equals("")) return 0;
    try {
      return Long.parseLong(o);
    } catch (NumberFormatException ex) {
      return 0;
    }
  }
  final static String getString(Map<String, Object> map, String key) {
    Object o = map.get(key);
    return o != null ? o.toString() : null;
  }
  final static boolean getBoolean(Map<String, Object> map, String key) {
    return getBoolean(map, key, false);
  }
  final static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
    Object o = map.get(key);
    if (o instanceof Boolean) {
      return (Boolean)o;
    } else {
      return defaultValue;
    }
  }
  public static class ItemFile {
    String name;
    String format;
    long mtime;
    long size;
    String md5;
    String crc32;
    String sha1;
//    public ItemFile(JSONObject jo) throws JSONException {
//      this.name = jo.getString("name");
//      this.format = jo.optString("format");
//      // these fields are returned as string.
//      this.mtime = Long.parseLong(jo.optString("mtime", "0"));
//      this.size = Long.parseLong(jo.optString("size", "0"));
//      this.md5 = jo.optString("md5");
//      this.crc32 = jo.optString("crc32");
//      this.sha1 = jo.optString("sha1");
//    }
    public ItemFile(Map<String, Object> jo) {
      this.name = (String)jo.get("name");
      this.format = (String)jo.get("format");
      // Metadata API returns these numeric values as strings
      this.mtime = parseLong(jo.get("mtime"));
      this.size = parseLong(jo.get("size"));
      this.md5 = (String)jo.get("md5");
      this.crc32 = (String)jo.get("crc32");
      this.sha1 = (String)jo.get("sha1");
    }
  }
  /**
   * Petabox Item metadata.
   * TODO: make this class top-level. it's likely to be reusable.
   *
   */
  public static class ItemMetadata {
    String server;
    String d1;
    String d2;
    String dir;
    String[] collection;
    Map<String, String> properties;
    long created;
    long updated;
    ItemFile[] files;
    boolean solo;
//    public ItemMetadata(JSONObject jo) {
//      this.server = jo.optString("server");
//      this.d1 = jo.optString("d1");
//      this.d2 = jo.optString("d2");
//      this.created = Long.parseLong(jo.optString("created", "0"));
//      this.updated = Long.parseLong(jo.optString("updated", "0"));
//      this.properties = new HashMap<String, String>();
//      JSONObject joprops = jo.optJSONObject("metadata");
//      if (joprops != null) {
//	for (String k : JSONObject.getNames(joprops)) {
//	  // all metadata/* except for collection has string value.
//	  if (k.equals("collection")) {
//	    JSONArray colls = joprops.optJSONArray(k);
//	    if (colls != null) {
//	      collection = new String[colls.length()];
//	      for (int i = 0; i < collection.length; i++) {
//		collection[i] = colls.optString(i);
//	      }
//	    }
//	  } else {
//	    this.properties.put(k, joprops.optString(k));
//	  }
//	}
//      }
//      JSONArray jofiles = jo.optJSONArray("files");
//      if (jofiles != null) {
//	this.files = new ItemFile[jofiles.length()];
//	for (int i = 0; i < jofiles.length(); i++) {
//	  JSONObject jofile = jofiles.optJSONObject(i);
//	  if (jofile != null) {
//	    try {
//	      this.files[i] = new ItemFile(jofile);
//	    } catch (JSONException ex) {
//	    }
//	  }
//	}
//      }
//    }
    @SuppressWarnings("unchecked")
    public ItemMetadata(Map<String, Object> jo) {
      // metadata API returns empty object ("{}") for non-existent item.
      // this can happen when item lookup is out of sync with actual system
      // (item deleted/lost, confused during shuffling, etc.). detect this
      // early and don't fail.
      if (jo.isEmpty()) return;
      
      this.server = getString(jo, "server");
      // for helping debug metadata API.
      if (this.server == null) {
	LOG.warn("jo=" + JSON.toString(jo));
      }
      this.d1 = getString(jo, "d1");
      this.d2 = getString(jo, "d2");
      this.created = parseLong(jo.get("created"));
      this.updated = parseLong(jo.get("updated"));
      this.solo = getBoolean(jo, "solo");
      this.properties = new HashMap<String, String>();
      Map<String, Object> joprops = (Map<String, Object>)jo.get("metadata");
      if (joprops != null) {
	for (String k : joprops.keySet()) {
	  // all metadata/* except for collection has string value.
	  if (k.equals("collection")) {
	    // collection member is a string if there's only one collection.
	    Object collection = joprops.get(k);
	    if (collection instanceof String) {
	      this.collection = new String[] { (String)collection };
	    } else if (collection != null) {
	      Object[] colls = (Object[])collection;
	      if (colls != null) {
		this.collection = new String[colls.length];
		for (int i = 0; i < this.collection.length; i++) {
		  this.collection[i] = colls[i].toString();
		}
	      }
	    }
	  } else {
	    this.properties.put(k, joprops.get(k).toString());
	  }
	}
      }
      Object[] jofiles = (Object[])jo.get("files");
      if (jofiles != null) {
	this.files = new ItemFile[jofiles.length];
	for (int i = 0; i < jofiles.length; i++) {
	  Map<String, Object> jofile = (Map<String, Object>)jofiles[i];
	  if (jofile != null) {
	    this.files[i] = new ItemFile(jofile);
	  }
	}
      }
    }
    @SuppressWarnings("unchecked")
    public ItemMetadata(Reader reader) throws /*JSONException, */IOException {
      //this(new JSONObject(new JSONTokener(reader)));
      this((Map<String, Object>)JSON.parse(reader));
    }
    public boolean isCollection() {
      return properties != null && "collection".equals(properties.get("mediatype"));
    }
  }

  protected ItemMetadata getItemMetadata(String itemid) throws IOException {
    if (itemid == null)
      throw new IOException("invalid itemid: null");
    if (itemid.equals(""))
      throw new IOException("invalid itemid \"" + itemid + "\"");
    ItemMetadata md = (ItemMetadata)metadataCache.get(itemid);
    if (md != null) return md;
    URI uri;
    try {
      uri = new URI("http", fsUri.getAuthority(), "/metadata/" + itemid, null);
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }
    HttpGet get = new HttpGet(uri);
    HttpParams params = get.getParams();
    HttpConnectionParams.setConnectionTimeout(params, metadataConnectionTimeout);
    HttpConnectionParams.setSoTimeout(params, metadataSocketTimeout);
    LOG.info("fetching metadata for item '" + itemid + "'");
    HttpEntity entity = null;
    int retries = 0;
    do {
      if (retries > 0) {
	if (retries > maxRetries) {
	  throw new IOException(uri + ": retry exhausted");
	}
	try {
	  Thread.sleep(retryDelay);
	} catch (InterruptedException ex) {
	}
      }
      HttpResponse resp;
      try {
	resp = client.execute(get);
      } catch (IOException ex) {
	// although getItemMetadata is declared as throws IOException, throwing IOException
	// will kill hadoop job. Request should be retried upon errors like "connection refused".
	LOG.warn(uri + " failed: " + ex.getMessage());
	++retries;
	continue;
      }
      StatusLine st = resp.getStatusLine();
      entity = resp.getEntity();
      switch (st.getStatusCode()) {
      case 200:
	if (retries > 0) {
	  LOG.info(uri + ": succeeded after " + retries + " retry(ies)");
	}
	break;
      case 502:
      case 503:
      case 504:
	entity.getContent().close();
	LOG.warn(uri + " failed " + st.getStatusCode() + " "
	    + st.getReasonPhrase() + ", try " + retries);
	++retries;
	entity = null;
	continue;
      default:
	entity.getContent().close();
	throw new IOException(uri + ": failed " + st.getStatusCode() + " "
	    + st.getReasonPhrase());
      }
      // XXX assuming JSON is in UTF-8 encoding
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      InputStream is = entity.getContent();
      int c;
      try {
	while ((c = is.read()) != -1) {
	  bao.write(c);
	}
      } catch (IOException ex) {
	LOG.warn("error reading metadata response (" + ex.getMessage() + ")");
	++retries;
	continue;
      } finally {
	is.close();
      }
      Reader reader = new InputStreamReader(new ByteArrayInputStream(bao.toByteArray()));
      //Reader reader = new InputStreamReader(entity.getContent(), "UTF-8");
      try {
	md = new ItemMetadata(reader);
      } catch (Throwable ex) {
	LOG.error("failed to parse matadata API response for item " + itemid + 
	    "(" + bao.size() + " bytes):\n" + bao.toString(), ex);
	throw new IOException("failed to parse metadata API response for item " + itemid, ex);
      }
      reader.close();
      if (md.server == null) {
	if (md.dir == null) {
	  // assume metadata API returned "{}", i.e. non-existent item.
	  if (++retries > maxRetries) {
	    // if ignore-missing-items flag is set, return with empty metadata. don't add it
	    // to the metadataCache.
	    if (ignoreMissingItems) {
	      break;
	    }
	    // throw specific exception for non-existent item case.
	    throw new FileNotFoundException("/" + itemid + ": non-existent item, retry exhausted");
	  }
	  LOG.warn("metadata API says item non-existent, retrying");
	  md = null;
	  continue;
	} else {
	  LOG.warn("metadata API failed (no server info) for item " + itemid + ", try " + retries);
	  LOG.warn("entity=" + new String(bao.toByteArray(), "UTF-8"));
	  ++retries;
	  md = null;
	  continue;
	}
      }
      metadataCache.put(itemid, md);
    } while (md == null);
    return md;
  }
  
  public void addAuthCookies(HttpMessage msg) {
    StringBuilder value = new StringBuilder();
    if (user != null) {
      value.append("logged-in-user=").append(user).append("; ");
      LOG.debug("logged-in-user=" + user);
    }
    if (sig != null) {
      value.append("logged-in-sig=").append(sig);
      LOG.debug("logged-in-sig=" + sig);
    }
    LOG.debug("adding auth cookies:" + value.toString());
    msg.addHeader("Cookie", value.toString());
  }
  public void setupRequest(HttpMessage msg) {
    msg.addHeader("User-Agent", USER_AGENT);
    msg.addHeader("Referer", getHadoopJobInfo());
    addAuthCookies(msg);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#append(org.apache.hadoop.fs.Path, int, org.apache.hadoop.util.Progressable)
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new ReadOnlyFileSystemException();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#create(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.permission.FsPermission, boolean, int, short, long, org.apache.hadoop.util.Progressable)
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    throw new ReadOnlyFileSystemException();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#delete(org.apache.hadoop.fs.Path)
   */
  @Override
  public boolean delete(Path f) throws IOException {
    throw new ReadOnlyFileSystemException();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#delete(org.apache.hadoop.fs.Path, boolean)
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new ReadOnlyFileSystemException();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#getFileStatus(org.apache.hadoop.fs.Path)
   * Path is mapped to Petabox structure as follows:
   * <ul>
   * <li><tt>/</tt><it>ITEMID</it>: directory
   * <li><tt>/</tt><it>ITEMID</it><tt>/</tt><it>FILENAME</it>: file
   * </ul>
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LOG.info("getFileStatus:" + f);
    FileStatus fstat = null;
    int depth = f.depth();
    if (depth == 1) {
      // Item
      String itemid = f.getName();
      ItemMetadata md = getItemMetadata(itemid);
      long mtime = md.updated;
      Path qf = makeQualified(f);
      // Note mtime is in seconds and FileStatus wants milliseconds.
      fstat = new FileStatus(0, true, md.solo ? 1 : 2, 4096, mtime * 1000, qf);
    } else if (depth == 2) {
      // Path is relative (more precisely, no scheme and authority) while Pig is 
      // enumerating target files, but it can be absolute in other use cases. 
      // Don't assume Path is relative.
      Path itempath = f.getParent();
      if (itempath == null) {
	// this should not happen because depth == 2
	throw new FileNotFoundException(f + "parent is null despite depth==2");
      }
      String itemid = itempath.getName();
      String fn = f.getName();
      ItemMetadata md = getItemMetadata(itemid);
      for (int i = 0; i < md.files.length; i++) {
	ItemFile ifile = md.files[i];
	if (ifile != null && fn.equals(ifile.name)) {
	  // we need to fully URI-qualified Path. Since f has path part only, it will
	  // be interpreted as a local/HDFS file.
	  Path qf = makeQualified(f);
	  // Note ifile.mtime is in seconds and FileStatus wants milliseconds.
	  fstat = new FileStatus(ifile.size, false, md.solo ? 1 : 2, 4096, ifile.mtime * 1000, qf);
	  break;
	}
      }
    } else {
      // file - currently only depth <= 2 is supported.
      throw new IOException("only 2-depth path is supported.");
    }
    return fstat;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#getUri()
   */
  @Override
  public URI getUri() {
    return fsUri;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#getWorkingDirectory()
   */
  @Override
  public Path getWorkingDirectory() {
    return cwd;
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
  
  public HttpClient getHttpClient() {
    return client;
  }
  
  /**
   * return HttpGet object properly setup with connection parameters and headers. 
   * @param uri request URI
   * @return configured HttpGet object
   */
  public HttpGet createHttpGet(URI uri) {
    HttpGet get = new HttpGet(uri);
    HttpParams params = get.getParams();
    HttpConnectionParams.setConnectionTimeout(params, connectionTimeout);
    HttpConnectionParams.setSoTimeout(params, socketTimeout);
    setupRequest(get);
    return get;
  }
  /**
   * enumerate items in collection {@code itemid} with Search Engine.
   * only returns index items (i.e. NoIndex items are excluded).
   * @param itemid collection identifier
   * @return array of FileStatus for (non-collection) items. 
   * @throws IOException
   */
  protected FileStatus[] searchItems(String itemid) throws IOException {
    return itemSearcher.searchItems(itemid);
  }
  
  private boolean accepted(ItemFile ifile) {
    if (fileTypes == null || fileTypes.length == 0) return true;
    if (ifile.format == null) return false;
    for (int i = 0; i < fileTypes.length; i++) {
      if (ifile.format.equals(fileTypes[i])) return true;
    }
    return false;
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#listStatus(org.apache.hadoop.fs.Path)
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    // TODO: we could optimize the query and return values, by inferring actually wanted files
    // from fsUri, which contains original FileSpec pattern string.
    int depth = f.depth();
    if (depth == 1) {
      // Item
      String itemid = f.getName();
      ItemMetadata md = getItemMetadata(itemid);
      // if itemid is a collection, list all items in the collection via search engine.
      // this returns FileStatuses with 1-depth canonical Path. Hadoop is just fine with it.  
      if (md.isCollection()) {
	return searchItems(itemid);
      }
      // for regular item, return list of files in it.
      if (md.files != null) {
	List<FileStatus> files = new ArrayList<FileStatus>();
	Path qf = makeQualified(f);
	for (int i = 0; i < md.files.length; i++) {
	  ItemFile ifile = md.files[i];
	  if (!accepted(ifile)) continue;
	  // perhaps blocksize should be much larger than this to prevent Hadoop from splitting input
	  // into overly small fragments, as range requests incur relatively high overhead.
	  files.add(new FileStatus(ifile.size, false, md.solo ? 1 : 2, 4096, ifile.mtime, new Path(qf, ifile.name)));
	}
	return files.toArray(new FileStatus[files.size()]);
      } else {
	// metadata API response had no "files" key - assume there's no files.
	return new FileStatus[0];
      }
    } else {
      // file - currently only depth 2 is supported.
      return new FileStatus[0];
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#mkdirs(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.permission.FsPermission)
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new ReadOnlyFileSystemException();
  }

  /**
   * HttpInputStream implements Seekable and PositionedReadable interfaces for HTTP
   * resources efficiently with Region requests.
   * Actual open is delayed until read operation is performed.
   *
   */
  public class HttpInputStream extends InputStream implements Seekable, PositionedReadable {
    protected URI uri;
    protected long pos;
    protected long endpos;
    protected InputStream in;
    /**
     * maximum length of seeking by reading off instead of re-opening resource
     * with new Range request. optimal value depends on the relative cost of
     * Range request.
     */
    public final int SMALL_GAP = 1000000;
    /**
     * 
     * @param uri actual HTTP URL to open.
     * @param bufferSize currently unused.
     */
    public HttpInputStream(URI uri, int bufferSize) {
      this.uri = uri;
      this.pos = 0;
      this.endpos = -1;
      this.in = null;
    }
    // Seekable
    public long getPos() throws IOException {
      return pos;
    }
    public void seek(long pos) throws IOException {
      if (this.pos == pos) return;
      if (in != null) {
	if (pos >= this.pos && pos <= this.pos + SMALL_GAP) {
	  int skiplen = (int)(pos - this.pos);
	  byte[] buffer = new byte[4096];
	  while (skiplen > 0) {
	    // TODO: should we let IOException from read to bubble up, or catch it and have
	    // read retry? decision depends on how Hadoop react to IOException from seek.
	    int n = in.read(buffer, 0, skiplen > buffer.length ? buffer.length : skiplen);
	    if (n < 0) break;
	    skiplen -= n;
	  }
	} else {
	  // moving backward or gap is larger than adequate for seek-by-reading
	  in.close();
	  in = null;
	}
      }
      this.pos = pos;
    }
    public boolean seekToNewSource(long targetPos) throws IOException {
      // TODO: check the detailed requirements for this method.
      return false;
    }
    
    /**
     * open actual InputStream for the resource, offset {@code pos}.
     * must set {@code in} to non-null if returning without throwing exception.
     * @throws IOException exception from underlying HTTP protocol
     */
    protected void open() throws IOException {
      HttpGet get = new HttpGet(uri);
      HttpParams params = get.getParams();
      HttpConnectionParams.setConnectionTimeout(params, connectionTimeout);
      HttpConnectionParams.setSoTimeout(params, socketTimeout);
      if (pos > 0) {
	get.addHeader("Range", "bytes=" + pos + "-");
      }
      setupRequest(get);
      LOG.info("HttpInputStream.open:" + uri + "(pos=" + pos + ")");
      int retries = 0;
      HttpEntity entity = null;
      do {
	if (retries > 0) {
	  try {
	    Thread.sleep(retryDelay);
	  } catch (InterruptedException ex) {
	  }
	}
	HttpResponse resp = null;
	try {
	  resp = client.execute(get);
	} catch (IOException ex) {
	  LOG.warn("connection to " + uri + " failed", ex);
	  if (++retries > maxRetries) {
	    throw new IOException(uri + ": retry exhausted trying to connect");
	  }
	  continue;
	}
	StatusLine st = resp.getStatusLine();
	entity = resp.getEntity();
	// TODO: detect failed Range request and report it. I know Range request is supported
	// on most resources, but It is catastrophic to ignore when it fails.
	switch (st.getStatusCode()) {
	case 200:
	case 206: // Partial Content
	  if (retries > 0) {
	    LOG.info(uri + ": succeeded after " + retries + " retry(ies)");
	  }
	  long clen = entity.getContentLength();
	  if (clen < 0) {
	    LOG.info("content-length is unavailable - no auto resume will be attempted.");
	    endpos = -1;
	  } else {
	    endpos = pos + entity.getContentLength();
	  }
	  break;
	case 404:
	  //entity.getContent().close();
	  // even 404 may be retried :-)
	  //throw new FileNotFoundException(uri + ": " + st.getReasonPhrase());
	case 403:
	  // petabox sometimes return false 403...
	  //entity.getContent().close();
	  //throw new IOException(uri + ": " + st.getReasonPhrase());
	case 500: // Internal Server Error
	case 502: // Bad Gateway
	case 503: // Service Unavailable
	case 504: // Gateway Timeout
	  // these happen when paired storage is overloaded, or having infrastructure-level
	  // problems and not rare.
	  entity.getContent().close();
	  if (++retries > maxRetries) {
	    throw new IOException(uri + ": retry exhausted on "
		+ st.getStatusCode() + " " + st.getReasonPhrase());
	  }
	  LOG.warn(uri + ": " + st.getStatusCode() + " " + st.getReasonPhrase()
	      + ", retry " + retries);
	  entity = null;
	  continue;
	default:
	  entity.getContent().close();
	  throw new IOException(uri + ": " + st.getStatusCode() + " " + st.getReasonPhrase());
	}
      } while (entity == null);
      in = entity.getContent();
    }
    @Override
    public void close() throws IOException {
      if (in != null) {
	in.close();
	in = null;
      }
    }
    @Override
    public int read() throws IOException {
      while (true) {
	if (in == null) open();
	int b;
	try {
	  b = in.read();
	} catch (ConnectionClosedException ex) {
	  // sender closed socket, probably for long idle period.
	  LOG.info("connection closed unexpectedly", ex);
	  b = -1;
	}
	if (b == -1) {
	  // if receiver/sender closed socket prematurely, try reopening.
	  if (endpos >= 0 && pos < endpos) {
	    LOG.info("socket closed prematurely. rereading from " + pos);
	    in = null;
	    continue;
	  }
	  return b;
	} else {
	  pos++;
	  return b;
	}
      }
    }
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      //LOG.info("read("+b+","+off+","+len+")");
      return super.read(b, off, len);
    }
    // PositionedReadable
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      LOG.info("HttpInputStream.read(" + position + "," + buffer.length + ")");
      // As PositionedReadable interface dictates this method does not change
      // file offset, and thread-safe, it is implemented as one-shot HTTP request.
      if (buffer.length == 0)  return 0;
      HttpGet get = new HttpGet(uri);
      HttpParams params = get.getParams();
      HttpConnectionParams.setConnectionTimeout(params, connectionTimeout);
      HttpConnectionParams.setSoTimeout(params, socketTimeout);
      setupRequest(get);
      get.addHeader("Range", "bytes=" + position + "-" + (position + length - 1));
      HttpEntity entity = null;
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
	  resp = client.execute(get);
	} catch (IOException ex) {
	  LOG.warn("connection to " + uri + " failed", ex);
	  if (++retries > maxRetries) {
	    throw new IOException(uri + ": retry exhausted trying to connect");
	  }
	  continue;
	}
	StatusLine st = resp.getStatusLine();
	entity = resp.getEntity();
	// TODO: detect failed Range request and report it. I know Range request is supported
	// on most resources, but It is catastrophic to ignore when it fails.
	switch (st.getStatusCode()) {
	case 200:
	  if (retries > 0) {
	    LOG.info(uri + ": succeeded after " + retries + " retry(ies)");
	  }
	  break;
	case 404:
	  //entity.getContent().close();
	  //throw new FileNotFoundException(st.getReasonPhrase());
	case 403:
	  // petabox often returns false 403...
	  //entity.getContent().close();
	  //throw new IOException(st.getReasonPhrase());
	case 502:
	case 503:
	case 504:
	  entity.getContent().close();
	  if (++retries > maxRetries) {
	    throw new IOException(uri + ": retry exhausted on "
		+ st.getStatusCode() + " " + st.getReasonPhrase());
	  }
	  LOG.warn(uri + ": " + st.getStatusCode() + " " + st.getReasonPhrase()
	      + ", retry " + retries);
	  entity = null;
	  continue;
	default:
	  entity.getContent().close();
	  throw new IOException(st.getReasonPhrase());
	}
      } while (entity == null);
      InputStream tin = entity.getContent();
      int n =  tin.read(buffer, offset, length);
      tin.close();
      return n;
    }
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
      // is there anything I should do if read bytes is less than length?
    }
  }
  /**
   * returns real URI for reading a resource represented by FileSystem URI {@code uri}.
   * {@link #urlTemplate} is used as template if non-null.
   * @param uri
   * @return
   * @throws URISyntaxException generated URI is in bad syntax (bad uriTemplate)
   */
  protected URI getRealURI(URI uri) throws URISyntaxException {
    if (urlTemplate == null) {
      return new URI("http", fsUri.getAuthority(), "/download" + uri.getPath(), null);
    }
    String path = uri.getPath(); // should be in /ITEM/FILE format.
    StringBuffer sb = new StringBuffer();
    // TODO: we could pre-parse urlTemplate for faster processing and early error reporting.
    Pattern p = Pattern.compile("%.");
    Matcher m = p.matcher(urlTemplate);
    while (m.find()) {
      switch (urlTemplate.charAt(m.end() - 1)) {
	case 'f':
	  m.appendReplacement(sb, path);
	  break;
	case '%':
	  m.appendReplacement(sb, "%");
	  break;
	default:
	  throw new URISyntaxException(urlTemplate, "unrecognized %-escape in urlTemplate:" + m.group());
      }
    }
    m.appendTail(sb);
    return URI.create(sb.toString());
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#open(org.apache.hadoop.fs.Path, int)
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    LOG.info("PetaboxFileSystem.open(" + f + ")");
    if (f.depth() == 1) {
      throw new IOException("is an item");
    }
    URI vURI = f.toUri();
    // XXX what if f has scheme and authority different from this FileSystem?
    // TODO: probably we should resolve item location by ourselves and let user put
    // preference on either primary or secondary.
    URI physURI;
    try {
      physURI = getRealURI(vURI);
    } catch (URISyntaxException ex) {
      throw new IOException("download URI construction failed: " + vURI.getPath());
    }
    return new FSDataInputStream(new HttpInputStream(physURI, bufferSize));
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#rename(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path)
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new ReadOnlyFileSystemException();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.FileSystem#setWorkingDirectory(org.apache.hadoop.fs.Path)
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    cwd = newDir;
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) return null;
    if (start < 0 || len < 0) throw new IllegalArgumentException("Invalid start or len parameter");
    if (file.getLen() < start) return new BlockLocation[0];
    // TODO: cache metadata. this method will be called for each input path. IA
    // metadata API is pretty fast, yet making identical queries is waste of cpu & bandwidth. 
    Path f = file.getPath();
    if (f.depth() == 2) {
      // Path is qualified.
      URI uri = f.toUri();
      String sf = uri.getPath();
      int ps = sf.indexOf(Path.SEPARATOR_CHAR, 1);
      String itemid = ps == -1 ? /* should not happen */ sf : sf.substring(1, ps);
      ItemMetadata md = getItemMetadata(itemid);
      List<String> name = new ArrayList<String>(2);
      List<String> host = new ArrayList<String>(2);
      if (md.d1 == null && md.d2 == null) {
	// unlikely to happen unless the item has just disappeared.
	if (md.server != null) {
	  name.add(md.server + ":80");
	  host.add(md.server);
	}
      } else {
	if (md.d1 != null && !md.d1.equals("")) {
	  name.add(md.d1 + ":80");
	  host.add(md.d1);
	}
	if (md.d2 != null && !md.d2.equals("")) {
	  name.add(md.d2 + ":80");
	  host.add(md.d2);
	}
      }
      LOG.debug("getFileBlockLocations sf=" + sf + " : " + host);
      if (name.isEmpty()) {
	return new BlockLocation[0];
      } else {
	return new BlockLocation[] {
	    new BlockLocation(name.toArray(new String[name.size()]),
		host.toArray(new String[host.size()]), 0, file.getLen())
	};
      }
    } else {
      return new BlockLocation[0];
    }
  }
}
