package org.archive.cassandra;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.sstable.SSTableSimpleWriter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.math.NumberUtils;
import org.archive.format.cdx.CDXLine;
import org.archive.format.cdx.StandardCDXLineFactory;
import org.archive.hadoop.mapreduce.CDXMapper;
import org.archive.util.zip.OpenJDK7GZIPInputStream;

public class CDXToSSTable {
	
	protected CDXMapper cdxConverter;
	
	ByteOrderedPartitioner partitioner;
	SSTableSimpleWriter writer;
	
	protected StandardCDXLineFactory cdxLineFactory;
	
	protected String lastKey;
	
	public CDXToSSTable(String dir, String format, String keyspace, String cf, ICompressor compressor, boolean conv)
	{
		partitioner = new ByteOrderedPartitioner();
		cdxLineFactory = new StandardCDXLineFactory(format);
		
		if (conv) {
			cdxConverter = new CDXMapper();
		}
		
		File fileDir = new File(dir);
		fileDir.mkdir();
		
		CFMetaData cfmeta = new CFMetaData(keyspace, cf, ColumnFamilyType.Standard, UTF8Type.instance, null);
		
		CompressionParameters cprop = new CompressionParameters(compressor);
		cfmeta.compressionParameters(cprop);
		
		writer = new SSTableSimpleWriter(fileDir, cfmeta, partitioner);
	}
	
	
	final static String DEFAULT_KEYSPACE = "cdxspace";
	final static String DEFAULT_CF = "cdx2";
	final static String DEFAULT_FORMAT = "cdx11";

	public static void main(String[] args) throws IOException {
		
		Options options = new Options();
		options.addOption("d", false, "DeflateCompressor");
		options.addOption("l", false, "LZ4Compressor");
		options.addOption("s", false, "SnappyCompressor");
		
		//options.addOption("o", "output", true, "Ouput Dir");
		options.addOption("k", "key", true, "Keyspace (default: " + DEFAULT_KEYSPACE + ")");
		options.addOption("f", "format", true, "cdx format (default: " + DEFAULT_FORMAT + ")");		
		options.addOption("cf", "table", true, "Table/Column Family (default: " + DEFAULT_CF + ")");	
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		
		try {
	        cmd = parser.parse( options, args);
        } catch (ParseException e) {
        	System.err.println(e);
        }
		
		String keyspace = cmd.getOptionValue("k", DEFAULT_KEYSPACE);
		String cf = cmd.getOptionValue("cf", DEFAULT_CF);
		String format = cmd.getOptionValue("f", DEFAULT_FORMAT);
		boolean convert = format.equals("cdx09");
		
		CDXToSSTable cdxTosst = null;

		ICompressor compressor = null;
		
		if (cmd.hasOption("d")) {
			compressor = DeflateCompressor.create(null);
		} else if (cmd.hasOption("s")) {
			compressor = SnappyCompressor.create(null);
		} else if (cmd.hasOption("l")) {
			compressor = LZ4Compressor.create(null);
		} else {
			compressor = LZ4Compressor.create(null);			
		}
		
		String argsleft[] = cmd.getArgs();
		
		if (argsleft.length == 0) {
			System.err.println("Must supply <outdir>");
			System.exit(1);
			return;
		}
		
		String input, outdir;
		
		if (argsleft.length == 1) {
			outdir = argsleft[0];
			input = null;
		} else {
			input = argsleft[0];
			outdir = argsleft[1];
		}
		
		
		try {
			Config.setClientMode(true);
			
			InputStream in = null;
			
			if (input == null || input.isEmpty() || input.equals("-")) {
				in = System.in;
			} else {
				in = new FileInputStream(input);
				if (input.endsWith(".gz")) {
					in = new OpenJDK7GZIPInputStream(in);
				}
			}
			
			cdxTosst = new CDXToSSTable(outdir, format, keyspace, cf, compressor, convert);
			
			String line = null;
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			while ((line = reader.readLine()) != null) {
				cdxTosst.insertCdxLine(line);
			}
			
		} finally {
			if (cdxTosst != null) {
				cdxTosst.close();
			}
		}
	}
	
	public void insertCdxLine(String cdxline) throws IOException
	{
		if (cdxline.startsWith("dns:") || cdxline.startsWith("warcinfo:")) {
			return;
		}
		
		if (cdxConverter != null) {
			try {
				cdxline = cdxConverter.convertLine(cdxline);
			} catch (Exception e) {
				//System.err.println("Skipping " + cdxline + " due to " + e.toString());
				return;
			}
		}
		
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
		
		long timestamp = System.currentTimeMillis();
		
		String key = surt + " " + datetime;
		
		if (lastKey != null && key.equals(lastKey)) {
			System.err.println("Skipping Dupe: " + key);
			return;
		}
		
		lastKey = key;
		
		writer.newRow(bytes(key));
		writer.addColumn(bytes("originalurl"), bytes(original), timestamp);
		writer.addColumn(bytes("mimetype"), bytes(mimetype), timestamp);
		writer.addColumn(bytes("statuscode"), bytes(statuscode), timestamp);
		writer.addColumn(bytes("digest"), bytes(digest), timestamp);			
		writer.addColumn(bytes("offset"), bytes(offset), timestamp);
		writer.addColumn(bytes("length"), bytes(length), timestamp);
		writer.addColumn(bytes("filename"), bytes(filename), timestamp);
	}
	
	public void close()
	{
		writer.close();
	}
}
