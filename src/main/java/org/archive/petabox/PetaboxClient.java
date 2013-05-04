package org.archive.petabox;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
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
import org.archive.hadoop.fs.PetaboxFileSystem;

public class PetaboxClient {
	private static Log LOG = LogFactory.getLog(PetaboxClient.class);

	protected String petaboxProtocol = "http";
	protected String petaboxHost = "archive.org";
	public void setPetaboxHost(String petaboxHost) {
		this.petaboxHost = petaboxHost;
	}
	/**
	 * text set to Referer header of each request.
	 * for helping petabox admin identify source of requests.
	 */
	protected String referer;
	public void setReferer(String referer) {
		this.referer = referer;
	}

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
	
	protected int bufferSize = 8192;
	
	/**
	 * if true, PetaboxFileSystem makes up empty item when Metadata API tells it's non-existent,
	 * instead of throwing FileNotFoundException.
	 */
	protected boolean ignoreMissingItems = false;

	public static final String VERSION = "0.0.2";
	public static final String USER_AGENT = PetaboxFileSystem.class.getName() + "/" + VERSION;

	protected PetaboxCredentialProvider credentialProvider;

	public void setCredentialProvider(PetaboxCredentialProvider credentialProvider) {
		this.credentialProvider = credentialProvider;
	}

	protected HttpClient client;

	public PetaboxClient(PetaboxClientConfig conf) {
		// ClientConnectionManager properties can be configured by config properties. 
		ThreadSafeClientConnManager connman = new ThreadSafeClientConnManager();
		int maxPerRoute = conf.getInt("max-per-route",
				connman.getDefaultMaxPerRoute());
		int maxTotal = conf.getInt("max-total",
				connman.getMaxTotal());
		connman.setDefaultMaxPerRoute(maxPerRoute);
		connman.setMaxTotal(maxTotal);
		this.client = new DefaultHttpClient(connman);

		this.maxRetries = conf.getInt("max-retries", this.maxRetries);
		this.retryDelay = conf.getInt("retry-delay", this.retryDelay);
		this.connectionTimeout = conf.getInt("connection-timeout", this.connectionTimeout);
		this.socketTimeout = conf.getInt("socket-timeout", this.socketTimeout);
		this.metadataConnectionTimeout = conf.getInt("metadata.connection-timeout",
				this.metadataConnectionTimeout);
		this.metadataSocketTimeout = conf.getInt("metadata.socket-timeout", 
				this.metadataSocketTimeout);
	    this.ignoreMissingItems = conf.getBoolean("ignore-missing-items", this.ignoreMissingItems);
	}
	
	public ItemMetadata getItemMetadata(String itemid) throws IOException {
		if (itemid == null)
			throw new IOException("invalid itemid: null");
		if (itemid.equals(""))
			throw new IOException("invalid itemid \"" + itemid + "\"");
		URI uri;
		try {
			uri = new URI(petaboxProtocol, petaboxHost, "/metadata/" + itemid, null);
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
		ItemMetadata md = null;
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
		if (referer != null)
			msg.addHeader("Referer", referer);
		addAuthCookies(msg);
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
	 * HttpInputStream implements MapReduce compatible InputStream on top of HTTP-based
	 * access to files on Petabox. It makes best effort to encapsulate retries often
	 * necessary to deal with transient problems.
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
		protected byte[] buffer;
		protected int bufpos;
		protected int bufend;
		
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
			this(uri, bufferSize, 0);
		}
		public HttpInputStream(URI uri, int bufferSize, long offset) {
			this.uri = uri;
			this.pos = offset;
			this.endpos = -1;
			this.in = null;
			if (bufferSize > 0) {
				buffer = new byte[bufferSize];
			}
		}
		// it is critical to override this method for GZIP decompression
		// to always work correctly on block-compressed (concatenated) file.
		// InputStream.available() always returns 0, which makes GZIP decompression
		// to assume there's no more concatenated blocks when there are <= 26
		// bytes left in its decompression buffer. see GZIPInputStream.readTrailer()
		// for details.
		@Override
		public int available() throws IOException {
			// as long as it is > 0, return value itself doesn't mean much.
			return pos < endpos ? 1 : 0;
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
					if (buffer != null) {
						if (bufpos + skiplen < bufend) {
							// new position is within the buffer
							bufpos += skiplen;
							skiplen = 0;
						} else {
							skiplen -= (bufend - bufpos);
						}
 					}
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
			HttpGet get = createHttpGet(uri);
			if (pos > 0) {
				get.addHeader("Range", "bytes=" + pos + "-");
			}
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
							+ ", retry " + retries + "/" + maxRetries);
					entity = null;
					if (st.getStatusCode() == 403) {
						// when we get false 403, it often 'sticks' to HttpClient because of
						// PHPSESSID cookie. Remove it before retrying.
						if (client instanceof DefaultHttpClient) {
							((DefaultHttpClient)client).getCookieStore().clear();
						}
					}
					continue;
				default:
					entity.getContent().close();
					throw new IOException(uri + ": " + st.getStatusCode() + " " + st.getReasonPhrase());
				}
			} while (entity == null);
			in = entity.getContent();
			if (buffer != null) {
				bufpos = buffer.length;
				bufend = 0;
			}
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
					if (buffer == null) {
						b = in.read();
					} else {
						if (bufpos >= buffer.length) {
							bufend = in.read(buffer);
							bufpos = 0;
						}
						if (bufpos >= bufend) {
							b = -1;
						} else {
							b = (buffer[bufpos++] & 0xff);
						}
					}
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
	
	protected HttpInputStream openURI(URI uri, long offset) throws URISyntaxException {
		String scheme = uri.getScheme();
		String authority = uri.getAuthority();
		if (scheme == null || authority == null) {
			if (scheme == null)
				scheme = petaboxProtocol;
			if (authority == null)
				authority = petaboxHost;
			uri = new URI(scheme, authority, uri.getPath(), uri.getQuery(), uri.getFragment());
		}
		return new HttpInputStream(uri, bufferSize, offset);
	}
	public HttpInputStream openURI(URI uri) throws URISyntaxException {
		return openURI(uri, 0);
	}

	/**
	 * return HttpInputStream for reading {@code urlpath} reliably.
	 * @param urlpath path part of an HTTP resource to read.
	 * @return HttpInputStream
	 * @throws URISyntaxException if urlpath is illegal as URI path
	 */
	protected HttpInputStream openPath(String urlpath, long offset) throws URISyntaxException {
		URI uri = new URI(petaboxProtocol, petaboxHost, urlpath, null);
		return new HttpInputStream(uri, bufferSize, offset);
	}
	protected HttpInputStream openPath(String urlpath) throws URISyntaxException {
		return openPath(urlpath, 0);
	}
	/**
	 * 
	 * @param path designates a file to download in /IDENTIFIER/FILE format.
	 * @return
	 * @throws URISyntaxException if path is illegal as URI path
	 */
	public HttpInputStream openDownload(String path) throws URISyntaxException {
		return openDownload(path, 0);
	}
	public HttpInputStream openDownload(String path, long offset) throws URISyntaxException {
		return openPath((path.startsWith("/") ? "/download" : "/download/") + path, offset);
	}
	
	// TEMPORARY - subject to refactoring in the near future.
	
	public HttpResponse doGet(URI uri) throws IOException {
		HttpGet get = createHttpGet(uri);
		return client.execute(get);
	}
}