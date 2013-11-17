package org.archive.cassandra;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.math.NumberUtils;
import org.archive.format.cdx.CDXLine;
import org.archive.format.cdx.StandardCDXLineFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class CDXImporter {
	
	protected Cluster cluster;
	
	protected Session session;
	
	protected String cdxQuery = 
	"INSERT INTO cdxspace.cdx (surt, datetime, originalurl, mimetype, statuscode, digest, offset, length, filename)" +
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	protected PreparedStatement insertCdxQuery;
	protected BatchStatement batch = null;
	
	protected int batchCount = 0;
	protected int numToBatch = 10000;
	
	protected StandardCDXLineFactory cdxLineFactory = new StandardCDXLineFactory("cdx11");
	
	protected PoolingOptions pool = new PoolingOptions();
	
	protected ResultSetFuture lastResult;

	protected int minuteTimeout = 3;
	

	public void init(String node) {
				
		Cluster.Builder builder = Cluster.builder().addContactPoint(node);
		builder.withCompression(Compression.LZ4);
		builder.withPoolingOptions(pool);
		
		cluster = builder.build();
		
		Metadata metadata = cluster.getMetadata();
		
		System.out.printf("Connected to cluster: %s\n",  metadata.getClusterName());
		
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
			        host.getDatacenter(), host.getAddress(), host.getRack());
		}
		
		session = cluster.connect();
		
		insertCdxQuery = session.prepare(cdxQuery);
		
	}
	
	public void insertCdxLine(String cdxline)
	{
		CDXLine line = cdxLineFactory.createStandardCDXLine(cdxline);
		
		
		String surt = line.getUrlKey();
		String datetime = line.getTimestamp();
		String original = line.getOriginalUrl();
		String mimetype = line.getMimeType();
		Integer statuscode = NumberUtils.toInt(line.getStatusCode(), -1);
		String digest = line.getDigest();
		Long offset = NumberUtils.toLong(line.getOffset(), -1);
		Integer length = NumberUtils.toInt(line.getLength(), -1);
		String filename = line.getFilename();
		
		BoundStatement cdxStmt = new BoundStatement(insertCdxQuery);
		cdxStmt.bind(surt, datetime, original, mimetype, statuscode, digest, offset, length, filename);
		
		if (batch == null) {
			batch = new BatchStatement(Type.UNLOGGED);
		}
		
		batch.add(cdxStmt);
		batchCount++;
		
		if (batchCount >= numToBatch) {
			sendBatch();
		}
	}
	
	protected void sendBatch()
	{
		if (lastResult != null) {
			try {
	            lastResult.getUninterruptibly(minuteTimeout, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
            	System.err.println(e.toString());
            }
		}
		
		lastResult = session.executeAsync(batch);
		batchCount = 0;
		batch = null;
	}
	
	public void close()
	{
		if (batch != null) {
			sendBatch();
		}
		
		boolean result = false;
		System.out.println("Starting Cluster Shutdown...");
		
		if (cluster != null) {
			try {
	            cluster.shutdown().get(3, TimeUnit.MINUTES);
            } catch (Exception e) {
        		System.out.println("Shutdown Interrupted!");
            }
		}
		
		System.out.println("Cluster Shutdown: " + result);
	}

	public String getCdxQuery() {
		return cdxQuery;
	}

	public void setCdxQuery(String cdxQuery) {
		this.cdxQuery = cdxQuery;
	}

	public StandardCDXLineFactory getCdxLineFactory() {
		return cdxLineFactory;
	}

	public void setCdxLineFactory(StandardCDXLineFactory cdxLineFactory) {
		this.cdxLineFactory = cdxLineFactory;
	}

	public int getNumToBatch() {
		return numToBatch;
	}

	public void setNumToBatch(int numToBatch) {
		this.numToBatch = numToBatch;
	}

	public PoolingOptions getPool() {
		return pool;
	}

	public void setPool(PoolingOptions pool) {
		this.pool = pool;
	}

	public int getMinuteTimeout() {
		return minuteTimeout;
	}

	public void setMinuteTimeout(int minuteTimeout) {
		this.minuteTimeout = minuteTimeout;
	}	
	
}
