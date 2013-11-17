package org.archive.cassandra;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.NumberUtils;
import org.archive.format.cdx.CDXLine;
import org.archive.format.cdx.StandardCDXLineFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CDXImporter {
	
	protected Cluster cluster;
	
	protected Session session;
	
	protected String cdxQuery = 
	"INSERT INTO cdxspace.cdx (surt, datetime, originalurl, mimetype, statuscode, digest, offset, length, filename)" +
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	protected PreparedStatement insertCdxQuery;
	
	protected StandardCDXLineFactory cdxLineFactory = new StandardCDXLineFactory("cdx11");

	public void init(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
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
		
		session.execute(cdxStmt);
	}
	
	public void close()
	{		
		boolean result = false;
		System.out.println("Starting Cluster Shutdown...");
		
		if (cluster != null) {
			result = cluster.shutdown(30, TimeUnit.SECONDS);
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
	
}
