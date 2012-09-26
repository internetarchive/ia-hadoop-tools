/**
 * 
 */
package org.archive.crawler.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.httpclient.URIException;
import org.archive.bdb.BdbModule;
import org.archive.bdb.KryoBinding;
import org.archive.crawler.frontier.BdbFrontier;
import org.archive.crawler.frontier.BdbMultipleWorkQueues;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.HTMLLinkContext;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.hq.HttpHeadquarterAdapter;
import org.archive.net.UURI;
import org.archive.spring.ConfigPath;
import org.archive.util.bdbje.EnhancedEnvironment;
import org.json.JSONException;
import org.json.JSONObject;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

/**
 * {@link DumpPending} reads to-be-crawled URLs left in "{@code pending}" BDB database
 * in state directory, and prints them in a format selected. output may be used for re-scheduling checked-out-but-not-crawled-yet URLs upon graceful
 * crawler shutdown.
 * 
 * Two output formats are supported currently:
 * <dl>
 * <dt>JSON
 * <dd>JSON format compatible with HQ's {@code schedule} tool. output may be submitted to
 * HQ so that they are fed to crawlers again.</dd>
 * <dt>Heritrix3 Import</dt>
 * <dd>Space-separated format compatible with Heritrix3 "recover format". output may be
 * placed in Heritrix3 action directory for rescheduling. This is now default and
 * recommended method.</dd> 
 * </dl>
 * Note that this class reads BDB database through low-level API. Future changes to following class
 * may break this code.
 * <ul>
 * <li>{@link BdbMultipleWorkQueues}
 * <li>{@link BdbFrontier}
 * </ul>
 * 
 * TODO: it would be best if Heritirix's core classes exposes APIs/constants useful for this tool.
 * The same goes with HQ.
 * 
 * @author kenji
 */
public class DumpPending {

  public static void printDatabaseNames(File dir, PrintStream out) {
    EnvironmentConfig econfig = new EnvironmentConfig();
    econfig.setAllowCreate(false);
    //econfig.setConfigParam("je.sharedCache", "true");
    EnhancedEnvironment bdbenv = new EnhancedEnvironment(dir, econfig);
    //StoredClassCatalog classCatalog = bdbenv.getClassCatalog();
    List<String> dbnames = bdbenv.getDatabaseNames();
    for (String dbname : dbnames) {
      out.println("DB:" + dbname);
    }
    bdbenv.close();
  }
  public static abstract class Formatter {
    public abstract void format(CrawlURI curi, PrintStream out) throws IOException;
  }
  public static final Formatter FORMAT_HQJSON = new Formatter() {
    public void format(CrawlURI curi, PrintStream out) {
      JSONObject jo = new JSONObject();
      try {
	jo.put(HttpHeadquarterAdapter.PROPERTY_URI, curi.getURI());
	String path = curi.getPathFromSeed();
	if (path != null && !path.equals("")) {
	  JSONObject jw = new JSONObject();
	  jw.put(HttpHeadquarterAdapter.PROPERTY_PATH, path);
	  UURI via = curi.getVia();
	  if (via != null) {
	    try {
	      jw.put(HttpHeadquarterAdapter.PROPERTY_VIA, via.getURI());
	    } catch (URIException ex) {
	      System.err.println("bad via ignored for " + curi);
	    }
	  }
	  LinkContext context = curi.getViaContext();
	  if (context != null) {
	    if (context instanceof HTMLLinkContext) {
	      jw.put(HttpHeadquarterAdapter.PROPERTY_CONTEXT, context.toString());
	    }
	  }
	  jo.put("w", jw);
	}
	out.println(jo.toString());
      } catch (JSONException ex) {
	System.err.println("error building JSON for " + curi + ": " + ex.getMessage());
      }
    }
  };
  /**
   * Formatter for Heritrix3 import compatible output. 
   * @see CrawlURI#fromHopsViaString(String)
   */
  public static final Formatter FORMAT_H3IMPORT = new Formatter() {
    public void format(CrawlURI curi, PrintStream out) throws IOException {
      UURI uuri = curi.getUURI();
      String uuriS = uuri.getEscapedURI();
      String path = curi.getPathFromSeed();
      if (path != null && !path.equals("")) {
	UURI via = curi.getVia();
	String viaS = via != null ? via.getEscapedURI() : "";
	LinkContext context = curi.getViaContext();
	String contextS = (context != null && (context instanceof HTMLLinkContext) ?
	    context.toString() : "");
	String line = "F+ " + uuriS + " " + path + " " + viaS + " " + contextS + "\n";
	out.write(line.getBytes("UTF-8"));
      } else {
	String line = "F+ " + uuriS + "\n";
	out.write(line.getBytes("UTF-8"));
      }
    }
  };
  /**
   * @param args
   */
  public static void main(String[] args) {
    // options
    Formatter formatter = FORMAT_H3IMPORT;
    boolean includePrerequisites = false;
    int ia;
    for (ia = 0; args.length > ia && args[ia].startsWith("-"); ia++) {
      if (args[ia].equals("-f")) {
	ia++;
	if (args[ia].equals("q"))
	  formatter = FORMAT_HQJSON;
	else if (args[ia].equals("s"))
	  formatter = FORMAT_H3IMPORT;
	else {
	  System.err.println("unrecognized value for -f option: " + args[ia]);
	  System.exit(1);
	}
      } else if (args[ia].equals("-P")) {
	includePrerequisites = true;
      } else {
	System.err.println("unrecognized option " + args[ia]);
	System.exit(1);
      }
    }
    if (ia == args.length) {
      System.err.println(DumpPending.class.getName() + " [-f {s|q}] STATE_DIRECTORY");
      System.exit(1);
    }
    File dir = new File(args[ia]);
    System.err.println("opening BDB in " + dir);
    if (!dir.isDirectory()) {
      System.err.println("directory does not exist.");
      System.exit(1);
    }
    System.err.println("databases found:");
    printDatabaseNames(dir, System.err);

    long t0 = System.currentTimeMillis();

    BdbModule bdb = new BdbModule();
    bdb.setDir(new ConfigPath("dir", dir.getPath()));
    bdb.start();
    BdbModule.BdbConfig dbConfig = new BdbModule.BdbConfig();
    dbConfig.setAllowCreate(false);
    // this is the database backing BdbMultipleWorkQueues.
    Database db = bdb.openDatabase("pending", dbConfig, true);

    EntryBinding<CrawlURI> crawlUriBinding = new KryoBinding<CrawlURI>(CrawlURI.class);

    Cursor cursor = db.openCursor(null, null);
    DatabaseEntry key = new DatabaseEntry();
    DatabaseEntry data = new DatabaseEntry();
    int n = 0;
    try {
      while (true) {
	OperationStatus status = cursor.getNext(key, data, null);
	if (status != OperationStatus.SUCCESS) {
	  break;
	}
	if (data.getSize() > 0) {
	  n++;
	  CrawlURI curi = (CrawlURI)crawlUriBinding.entryToObject(data);
	  if (!includePrerequisites) {
	    String linkpath = curi.getPathFromSeed();
	    int l = linkpath.length();
	    if (l > 0 && linkpath.charAt(l - 1) == 'P')
	      continue;
	    // just in case above doesn't work
	    if ("dns".equals(curi.getUURI().getScheme()))
	      continue;
	    String path = curi.getUURI().getPath();
	    if (path != null && path.equals("/robots.txt"))
	      continue;
	  }
	  //	ObjectBuffer ob = new ObjectBuffer(kryo, 16*1024, Integer.MAX_VALUE);
	  //	CrawlURI curi = ob.readObjectData(data.getData(), CrawlURI.class);
	  //	System.out.println(n + ": " + curi);
	  formatter.format(curi, System.out);
	}
	System.err.format("\r%d", n);
      }
    } catch (IOException ex) {
      System.err.println("error writing output:" + ex.getMessage());
    }
    System.err.println();
    cursor.close();
    bdb.close();
    bdb.stop();

    long secs = System.currentTimeMillis() - t0;
    System.err.format("CrawlURIs dumped: %d in %.3fs\n", n, secs / 1000.0);
  }

}
