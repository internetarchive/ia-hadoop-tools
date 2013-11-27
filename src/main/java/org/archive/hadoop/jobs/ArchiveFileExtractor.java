/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.archive.hadoop.jobs;

import com.google.common.io.ByteStreams;
import com.google.common.io.LimitInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.archive.extract.ExtractingResourceFactoryMapper;
import org.archive.extract.ExtractingResourceProducer;
import org.archive.extract.ExtractorOutput;
import org.archive.extract.ProducerUtils;
import org.archive.extract.ResourceFactoryMapper;
import org.archive.extract.WATExtractorOutput;
import org.archive.format.gzip.GZIPFormatException;
import org.archive.format.gzip.GZIPMemberSeries;
import org.archive.format.gzip.GZIPMemberWriter;
import org.archive.format.gzip.GZIPSeriesMember;
import org.archive.hadoop.util.FilenameInputFormat;
import org.archive.resource.Resource;
import org.archive.resource.ResourceProducer;
import org.archive.resource.producer.ARCFile;
import org.archive.resource.producer.EnvelopedResourceFile;
import org.archive.resource.producer.WARCFile;
import org.archive.streamcontext.SimpleStream;
import org.archive.util.DateUtils;
import org.archive.util.FileNameSpec;
import org.archive.util.HMACSigner;
import org.archive.util.IAUtils;
import org.archive.util.StringFieldExtractor.StringTuple;
import org.archive.util.StringFieldExtractor;

import org.archive.server.FileBackedInputStream;



/**
* ArchiveFileExtractor - Generate WAT files from (W)ARC files stored in HDFS
*/
public class ArchiveFileExtractor extends Configured implements Tool {
	
	public final static String TOOL_NAME = "ArchiveFileExtractor";
	public final static String TOOL_DESCRIPTION = "Repackage records from ARC/WARC files into new ARC/WARC files in HDFS";
	
	public static final Log LOG = LogFactory.getLog( ArchiveFileExtractor.class );


	public static class ArchiveFileExtractorMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
		private JobConf jobConf;
		
		private static final Charset UTF8 = Charset.forName("UTF-8");
		private static int CR = 13;
		private static int LF = 10;
		
		private String timestamp14;
		private String timestampZ;
		
		

		private byte[] warcHeaderContents;

		private final static String ARC_PATTERN = 
		"filedesc://%s 0.0.0.0 %s text/plain 76\n" +
		"1 0 InternetArchive\n" +
		"URL IP-address Archive-date Content-type Archive-length\n\n";

		private final static String WARC_PATTERN =
		"WARC/1.0\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: %s\r\n" +
		"WARC-Filename: %s\r\n" +
		"WARC-Record-ID: <urn:uuid:%s>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: %d\r\n\r\n";

		private String getWARCRecordID() {
			return "urn:uuid:" + UUID.randomUUID().toString();
		}
		
		private byte[] getARCHeader(String name) {
			return String.format(ARC_PATTERN,name,timestamp14).getBytes(UTF8);
		}

		private byte[] getWARCHeader(String name)  throws IOException {
			String t = String.format(WARC_PATTERN,
					timestampZ,name,getWARCRecordID(),warcHeaderContents.length + 4);
			byte[] b = t.getBytes(UTF8);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(b);
			baos.write(warcHeaderContents);
			baos.write(CR);
			baos.write(LF);
			baos.write(CR);
			baos.write(LF);
			return baos.toByteArray();
		}

		public byte[] getWarcHeaderContents() {
			return warcHeaderContents;
		}

		public void setWarcHeaderContents(byte[] warcHeaderContents) {
			this.warcHeaderContents = warcHeaderContents;
		}

		private long getGZLength(InputStream is) 
			throws IOException, GZIPFormatException {
			
			SimpleStream s = new SimpleStream(is);
			GZIPMemberSeries gzs = new GZIPMemberSeries(s,"range",0,true);
			GZIPSeriesMember m = gzs.getNextMember();
			m.skipMember();
			return m.getCompressedBytesRead();
		}
		
	
		/**
	  	* <p>Configures the job.</p>
	  	* * @param job The job configuration.
		*/

		public void configure( JobConf job ) {
			this.jobConf = job;
		}

		/**
		* Generate WAT file for the (w)arc file named in the
		* <code>key</code>
		*/
		
		public void map( Object key, Text value, OutputCollector output, Reporter reporter )throws IOException {
			
			String inputString = value.toString();
			String[] inputParts = inputString.split("\t"); 
			
			if(inputParts.length != 2) {
				throw new IOException ("invalid input");
			}
		
			boolean openArc = false;
			boolean openWarc = false;
			
			FileNameSpec warcNamer;
			FileNameSpec arcNamer;

			String hmacName = this.jobConf.get("hmacName","");
			String hmacSignature = this.jobConf.get("hmacSignature","");
			
			HMACSigner signer = null;
			if(hmacName != null && hmacSignature != null && !hmacName.isEmpty() && !hmacSignature.isEmpty())
				signer = new HMACSigner(hmacSignature, hmacName);
			
			timestamp14 = this.jobConf.get("timestamp14", DateUtils.get14DigitDate(System.currentTimeMillis()));
			String warcHeaderString = this.jobConf.get("warcHeaderString");
			warcHeaderContents = warcHeaderString.getBytes(UTF8);
			
			String outputDir = this.jobConf.get("outputDir");
			
			FileSystem hdfsSys = null;
	
			FSDataOutputStream currentArcOS = null;
			FSDataOutputStream currentWarcOS = null;

			try { 
				long msse = DateUtils.parse14DigitDate(timestamp14).getTime();
				timestampZ = DateUtils.getLog17Date(msse);
			} catch (ParseException e) {
				LOG.error( "Error parsing timestamp: ", e );
				throw new IOException( e );
			}
			
			String prefix = inputParts[0];
			prefix+="-";
			
			String resourceLocationBagString = inputParts[1];
			
			arcNamer = new FileNameSpec(prefix, ".arc.gz");
			warcNamer = new FileNameSpec(prefix, ".warc.gz");
			
			boolean firstArcRecord = true;
			boolean firstWarcRecord = true;
			
			//remove braces
			resourceLocationBagString = resourceLocationBagString.replaceAll("[{}]","");
			
			//split into tuples
			resourceLocationBagString = resourceLocationBagString.replace("),(", ")\t(");
			
			//remove parentheses
			resourceLocationBagString = resourceLocationBagString.replaceAll("[()]","");
			
			//inputs
			String[] resourceLocations = resourceLocationBagString.split("\t");
		
			FileBackedInputStream fbis = null;
			InputStream is = null;
			
			for (int i=0; i< resourceLocations.length; i++) {
				
				String[] offLoc = resourceLocations[i].split(",");
				long offset = Long.parseLong(offLoc[0]);
				String url = offLoc[1];
				
				boolean isArc = false;
					
				try { 
					if(url.endsWith(".arc.gz")) {
						isArc = true;
					} else if(url.endsWith(".warc.gz")) {		

					} else {
						throw new IOException("URL (" + url +
						") must end with '.arc.gz' or '.warc.gz'");
					}
				
					if(url.startsWith("http://")) {
						URL u = new URL(url);
						URLConnection conn = u.openConnection();
						conn.setRequestProperty("Range", String.format("bytes=%d-", offset));
						if(signer != null)	
							conn.setRequestProperty("Cookie", signer.getHMacCookieStr(1000));
						conn.connect();
						is = conn.getInputStream();
					} else if(url.startsWith("hdfs://")){
						URI u = new URI(url);
						//only initialize the FS once
						if (hdfsSys == null) {
							URI defaultURI = new URI(u.getScheme() + "://" + u.getHost() + ":"+ u.getPort() + "/");
							hdfsSys = FileSystem.get(defaultURI, new Configuration());
						}
						Path path = new Path(u.getPath());
						FSDataInputStream fis = hdfsSys.open(path);
						fis.seek(offset);
						is = fis;
					}
					
					fbis = new FileBackedInputStream(is);
					long length = getGZLength(fbis);
					InputStream orig = fbis.getInputStream();
					if(isArc) {
						if(firstArcRecord) {
							String newArcName = arcNamer.getNextName();
							String outputFileString = this.jobConf.get("outputDir") + "/" + newArcName;
							currentArcOS = FileSystem.get( new java.net.URI( outputFileString ), this.jobConf ).create(new Path (outputFileString), false); 
							byte[] header = getARCHeader(newArcName);
							GZIPMemberWriter w = new GZIPMemberWriter(currentArcOS);
   	     					w.write(new ByteArrayInputStream(header));
							firstArcRecord = false;
						}
						LimitInputStream lis = new LimitInputStream(orig, length);
						ByteStreams.copy(lis, currentArcOS);
					} else {
						if(firstWarcRecord) {
							String newWarcName = warcNamer.getNextName();
							String outputFileString = this.jobConf.get("outputDir") + "/" + newWarcName;
							currentWarcOS = FileSystem.get( new java.net.URI( outputFileString ), this.jobConf ).create(new Path (outputFileString), false); 
							byte[] header = getWARCHeader(newWarcName);
							GZIPMemberWriter w = new GZIPMemberWriter(currentWarcOS);
           					w.write(new ByteArrayInputStream(header));
							firstWarcRecord = false;
						}
						LimitInputStream lis = new LimitInputStream(orig, length);
						ByteStreams.copy(lis, currentWarcOS);
					}
					output.collect("SUCCESS",offset + "\t" + url);
				} catch (Exception e) {
					LOG.error( "Error processing: ", e );
					output.collect("FAIL",offset + "\t" + url);
					if ( ! this.jobConf.getBoolean( "soft", false ) ) {
						throw new IOException( e.toString() + "offset:" + offset + "url:" + url );
					}
				} finally {
					if(is != null) {
						is.close();
					}
					if(fbis != null) {
						fbis.resetBacker();
					}
				}
			}
			
			// end of for loop
			if(currentArcOS != null)
				currentArcOS.close();
			if(currentWarcOS != null)
				currentWarcOS.close();
		} 
	}

	/** 
	* Print usage
	*/
	public void printUsage() {
		String usage = "Usage: ArchiveFileExtractor [OPTIONS] <taskfile> <outputdir>\n";
		usage+="\tOptions:\n";
		usage+="\t\t-mappers NUM - try to run with approximately NUM map tasks\n";
		usage+="\t\t-timestamp14 TS - The 14 digit timestamp to use\n";
		usage+="\t\t-hmacname HMACNAME - The HMAC Name string\n";
		usage+="\t\t-hmacsignature HMACSIG - The HMAC Signature string\n";
		usage+="\t\t-warc-header-local-file LOCALPATH_TO_WARCHEADERFILE - The local file containing the WARC header to use\n";
		usage+="\t\t-soft - tolerate some task failures\n";
		usage+="\tThe taskfile contains lines of the form:\n";
		usage+="\t\tFilePrefix<tab>Bag of (offset,FilePath) tuples\n";
		usage+="\t\tFilePrefix is the prefix to be used by the extracted files\n";
		usage+="\t\toffset is the start offset of a W/ARC record\n";
		usage+="\t\tFilePath is a HTTP or HDFS URL to the file to extract from\n";
		System.out.println(usage);
	}
	
	/**
	* Run the job.
	*/
	public int run( String[] args ) throws Exception {
		if ( args.length < 2 ) {
			printUsage();
			return 1;
		}

		// Create a job configuration
		JobConf job = new JobConf( getConf( ) );

		// Job name uses output dir to help identify it to the operator.
		job.setJobName( "Archive File Extractor" );

		// This is a map-only job, no reducers.
		job.setNumReduceTasks(0);

		// turn off speculative execution
        job.setBoolean("mapred.map.tasks.speculative.execution",false);

		// set timeout to a high value - 20 hours
		job.setInt("mapred.task.timeout",72000000);
	
		job.setBoolean("soft",false);

		int arg = 0;
		int numMaps = 10;
		
		String DEFAULT_WARC_PATTERN = "software: %s Extractor\r\n" +
		"format: WARC File Format 1.0\r\n" +
		"conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n" +
		"publisher: Internet Archive\r\n" +
		"created: %s\r\n\r\n";
		
		String warcHeaderString = String.format(
			DEFAULT_WARC_PATTERN, 
			IAUtils.COMMONS_VERSION, 
			DateUtils.getLog17Date(System.currentTimeMillis()));
		
		while (arg < args.length -1) {
			if(args[arg].equals("-soft")) {
				job.setBoolean("soft",true);
				arg++;
			} else if(args[arg].equals("-mappers")) {
				arg++;
				numMaps = Integer.parseInt(args[arg]);
				job.setNumMapTasks(numMaps);
				arg++;
			} else if(args[arg].equals("-timestamp14")) {
				arg++;
				String timestamp14 = DateUtils.get14DigitDate(DateUtils.parse14DigitDate(args[arg]));
				job.set("timestamp14",timestamp14);
				arg++;
			} else if(args[arg].equals("-warc-header-local-file")) {
				arg++;
				File f = new File(args[arg]);
	    		FileInputStream fis = new FileInputStream(f);
	    		warcHeaderString = IOUtils.toString(fis, "UTF-8");
		   		arg++;
			} else if(args[arg].equals("-hmacname")) {
				arg++;
				String hmacName = args[arg];
				job.set("hmacName",hmacName);
				arg++;
			} else if(args[arg].equals("-hmacsignature")) {
				arg++;
				String hmacSignature = args[arg];
				job.set("hmacSignature",hmacSignature);
				arg++;
			} else {
				break;
			}
		}
		
		job.set("warcHeaderString",warcHeaderString);
	
		if(args.length - 2 != arg) {
			printUsage();
			return 1;
		}

		Path inputPath = new Path(args[arg]);
		arg++;

		String outputDir = args[arg];
		arg++;

		job.set("outputDir",outputDir);
		Path outputPath = new Path(outputDir);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ArchiveFileExtractorMapper.class);
		job.setJarByClass(ArchiveFileExtractor.class);

		
		TextInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	
		// Run the job!
		RunningJob rj = JobClient.runJob( job );
		if ( ! rj.isSuccessful( ) ) {
			LOG.error( "FAILED: " + rj.getID() );
			return 2;
		}
		return 0;
	}

	
	/**
	* Command-line driver.  Runs the ArchiveFileExtractor as a Hadoop job.
	*/
	public static void main( String args[] ) throws Exception {
		int result = ToolRunner.run(new Configuration(), new ArchiveFileExtractor(), args);
		System.exit( result );
	}

}
