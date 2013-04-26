/**
 * 
 */
package org.archive.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpMessage;
import org.apache.http.client.HttpClient;
import org.archive.hadoop.util.HadoopUtil;
import org.archive.petabox.CookieFilePetaboxCredentialProvider;
import org.archive.petabox.ItemFile;
import org.archive.petabox.ItemMetadata;
import org.archive.petabox.PetaboxClient;
import org.archive.petabox.PetaboxClientConfig;
import org.archive.petabox.PetaboxCredentialProvider;

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

	private static PetaboxFileSystem defaultInstance;

	protected PetaboxClient pbclient;

	protected String urlTemplate = null;

	//  protected HttpClient client;

	private String hadoopJobInfo;

	// metadata cache
	private LRUMap metadataCache = new LRUMap(200);


	private Class<? extends ItemSearcher> itemSearcherClass = SearchEngineItemSearcher.class;
	protected ItemSearcher itemSearcher;

	/**
	 * if non-null and non-emtpy, {@link #listStatus(Path)} will only return
	 * those files of specified media types (unless explicitly specified).
	 */
	protected String[] fileTypes;

	public PetaboxFileSystem() {
	}

	public static class HadoopPetaboxClientConfig implements PetaboxClientConfig {
		private Configuration conf;
		private String confbase;
		public HadoopPetaboxClientConfig(Configuration conf, String confbase) {
			this.conf = conf;
			this.confbase = confbase;
		}
		@Override
		public String getString(String name) {
			return conf.get(confbase + "." + name);
		}
		@Override
		public int getInt(String name, int defaultValue) {
			return conf.getInt(confbase + "." + name, defaultValue);
		}
		@Override
		public boolean getBoolean(String name, boolean defaultValue) {
			return conf.getBoolean(confbase + "." + name, defaultValue);
		}
	}

	@Override
	public void initialize(URI name, final Configuration conf) throws IOException {
		super.initialize(name, conf);
		// name.path is not necessarily be "/". it typically is what's passed to LOAD (i.e.
		// may contain wild-cards.)
		this.fsUri = name;
		this.cwd = new Path("/");

		final String confbase = "fs." + fsUri.getScheme();

		PetaboxClientConfig pbconf = new HadoopPetaboxClientConfig(conf, confbase);	
		this.pbclient = new PetaboxClient(pbconf);
		this.pbclient.setPetaboxHost(fsUri.getAuthority());
		this.pbclient.setReferer(getHadoopJobInfo());
		this.pbclient.setCredentialProvider(new PetaboxCredentialProvider() {
			private String user = conf.get(confbase + ".user");
			private String sig = conf.get(confbase + ".sig");
			private PetaboxCredentialProvider provider = new CookieFilePetaboxCredentialProvider();
			/**
			 * read user credentials from ~/.iaauth file with IA cookies.
			 */
			private void getCredentials() {
				this.user = provider.getUser();
				if (this.user != null) {
					conf.set("fs."+fsUri.getScheme()+".user", this.user);
				}
				this.sig = provider.getSignature();
				if (this.sig != null) {
					conf.set("fs."+fsUri.getScheme()+".sig", this.sig);
				}
				if (this.user == null || this.sig == null) {
					LOG.warn("did not load petabox authentication from cookies file");
				}
			}
			@Override
			public String getUser() {
				if (user == null)
					getCredentials();
				return user;
			}

			@Override
			public String getSignature() {
				if (user == null)
					getCredentials();
				return sig;
			}
		});


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

		LOG.info("PetaboxFileSystem.initialize:fsUri=" + fsUri);
		if (defaultInstance == null)
			defaultInstance = this;
	}

	/**
	 * return the first PetaboxFileSystem initialized in this JVM.
	 * convenience method for Hadoop scripting classes that want to provide
	 * quick access to PetaboxFileSystem instance.
	 * @return PetaboxFileSystem or null if no instance has been initialized yet.
	 */
	public static PetaboxFileSystem getDefaultInstance() {
		return defaultInstance;
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
	protected ItemMetadata getItemMetadata(String itemid) throws IOException {
		ItemMetadata md = (ItemMetadata)metadataCache.get(itemid);
		if (md == null) {
			md = pbclient.getItemMetadata(itemid);
			if (md != null) {
				metadataCache.put(itemid, md);
			}
		}
		return md;
	}

	public void setupRequest(HttpMessage msg) {
		pbclient.setupRequest(msg);
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
			long mtime = md.getUpdated();
			Path qf = makeQualified(f);
			// Note mtime is in seconds and FileStatus wants milliseconds.
			fstat = new FileStatus(0, true, md.isSolo() ? 1 : 2, 4096, mtime * 1000, qf);
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
			for (int i = 0; i < md.getFiles().length; i++) {
				ItemFile ifile = md.getFiles()[i];
				if (ifile != null && fn.equals(ifile.getName())) {
					// we need to fully URI-qualified Path. Since f has path part only, it will
					// be interpreted as a local/HDFS file.
					Path qf = makeQualified(f);
					// Note ifile.mtime is in seconds and FileStatus wants milliseconds.
					fstat = new FileStatus(ifile.getSize(), false, md.isSolo() ? 1 : 2, 4096, 
							ifile.getMtimeMS(), qf);
					break;
				}
			}
		} else {
			// file - currently only depth <= 2 is supported.
			throw new IOException(f.toString() + ": only 2-depth path is supported.");
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

	/**
	 * return PetaboxClient setup for this instance.
	 * this method may be removed by refactoring in near future.
	 * @return
	 */
	public PetaboxClient getPetaboxClient() {
		return pbclient;
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
		if (ifile.getFormat() == null) return false;
		for (int i = 0; i < fileTypes.length; i++) {
			if (ifile.getFormat().equals(fileTypes[i])) return true;
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
			if (md.getFiles() != null) {
				List<FileStatus> files = new ArrayList<FileStatus>();
				Path qf = makeQualified(f);
				for (int i = 0; i < md.getFiles().length; i++) {
					ItemFile ifile = md.getFiles()[i];
					if (!accepted(ifile)) continue;
					// perhaps blocksize should be much larger than this to prevent Hadoop from splitting input
					// into overly small fragments, as range requests incur relatively high overhead.
					files.add(new FileStatus(ifile.getSize(), false, md.isSolo() ? 1 : 2, 4096, 
							ifile.getMtimeMS(), new Path(qf, ifile.getName())));
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
			return new FSDataInputStream(pbclient.openURI(physURI));
		} catch (URISyntaxException ex) {
			throw new IOException("download URI construction failed: " + vURI.getPath());
		}
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
			if (md.getD1() == null && md.getD2() == null) {
				// unlikely to happen unless the item has just disappeared.
				if (md.getServer() != null) {
					name.add(md.getServer() + ":80");
					host.add(md.getServer());
				}
			} else {
				if (md.getD1() != null && !md.getD1().equals("")) {
					name.add(md.getD1() + ":80");
					host.add(md.getD1());
				}
				if (md.getD2() != null && !md.getD2().equals("")) {
					name.add(md.getD2() + ":80");
					host.add(md.getD2());
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
