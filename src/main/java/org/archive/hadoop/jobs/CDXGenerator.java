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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.archive.hadoop.util.FilenameInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.archive.extract.ExtractingResourceFactoryMapper;
import org.archive.extract.ExtractingResourceProducer;
import org.archive.extract.ExtractorOutput;
import org.archive.extract.ProducerUtils;
import org.archive.extract.ResourceFactoryMapper;
import org.archive.extract.RealCDXExtractorOutput;
import org.archive.resource.Resource;
import org.archive.resource.ResourceProducer;
import org.archive.util.StringFieldExtractor;
import org.archive.util.StringFieldExtractor.StringTuple;
import org.archive.resource.ResourceProducer;
import org.archive.resource.producer.ARCFile;
import org.archive.resource.producer.EnvelopedResourceFile;
import org.archive.resource.producer.WARCFile;
import java.lang.*;
import org.apache.commons.io.FilenameUtils;
import java.io.PrintWriter;

/**
* CDXGenerator - Generate CDX files from (W)ARC files stored in HDFS
*/
public class CDXGenerator extends Configured implements Tool {
	
	public final static String TOOL_NAME = "CDXGenerator";
	public final static String TOOL_DESCRIPTION = "Generate CDX files from (W)ARC files stored in HDFS";
	
	public static final Log LOG = LogFactory.getLog( CDXGenerator.class );

	public static class CDXGeneratorMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		private JobConf jobConf;

		/**
	  	* <p>Configures the job.</p>
	  	* * @param job The job configuration.
		*/

		public void configure( JobConf job ) {
			this.jobConf = job;
		}

		/**
		* Generate CDX file for the (w)arc file named in the
		* <code>key</code>
		*/
		public void map( Text key, Text value, OutputCollector output, Reporter reporter )throws IOException {
			String path = key.toString();
			LOG.info( "Start: "  + path );
			
			FSDataInputStream fis = null;
			Path inputPath = null;
			String inputBasename = null;
			String outputBasename = null;
			//get input stream
			try {
				inputPath = new Path(path);
				fis = FileSystem.get( new java.net.URI( path ), this.jobConf ).open( inputPath );
			
				inputBasename = inputPath.getName();
				
				if(path.endsWith(".gz")) {
					outputBasename = inputBasename.substring(0,inputBasename.length()-3) + ".cdx";
				} else {
					outputBasename = inputBasename + ".cdx";
				}
			} catch (Exception e) {
				LOG.error( "Error opening input file to process: " + path, e );
				throw new IOException( e );
			}

			String outputFileString = null;
			FSDataOutputStream fsdOut = null;

			//open output file handle
			try {
				outputFileString = this.jobConf.get("outputDir") + "/" + outputBasename;
				fsdOut = FileSystem.get( new java.net.URI( outputFileString ), this.jobConf ).create(new Path (outputFileString), false);
			} catch (Exception e) {
				LOG.error( "Error opening output file: " + outputFileString, e );
				throw new IOException( e );
			} 
			// data generation phase
			try {
				ExtractorOutput out;
				out = new RealCDXExtractorOutput(new PrintWriter(fsdOut));
				ResourceProducer producer = ProducerUtils.getProducer(path.toString());
				ResourceFactoryMapper mapper = new ExtractingResourceFactoryMapper();
				ExtractingResourceProducer exProducer = new ExtractingResourceProducer(producer, mapper);

				int count = 0;
				while(count < Integer.MAX_VALUE) {
					Resource r = exProducer.getNext();
					if(r == null) {
						break;
					}
					count++;
					out.output(r);
				}
				fsdOut.close();
			} catch ( Exception e ) {
				LOG.error( "Error processing file: " + path, e );
				if ( ! this.jobConf.getBoolean( "soft", false ) ) {
					throw new IOException( e );
				}
			} finally {
				LOG.info( "Finish: "  + path );
			}
		}
	}

	/**
	* Run the job.
	*/
	public int run( String[] args ) throws Exception {
		if ( args.length < 2 ) {
			usage();
			return 1;
		}

		// Create a job configuration
		JobConf job = new JobConf( getConf( ) );

		// Job name uses output dir to help identify it to the operator.
		job.setJobName( "CDX Generator " + args[0] );

		// The inputs are a list of filenames, use the
		// FilenameInputFormat to pass them to the mappers.
		job.setInputFormat( FilenameInputFormat.class );

		// This is a map-only job, no reducers.
		job.setNumReduceTasks(0);

		// turn off speculative execution
		job.setBoolean("mapred.map.tasks.speculative.execution",false);

		// set timeout to a high value - 20 hours
		job.setInt("mapred.task.timeout",72000000);
	
		//fail on task failures
		job.setBoolean("soft",false);
	
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CDXGeneratorMapper.class);
		job.setJarByClass(CDXGenerator.class);

		int arg = 0;
		if(args[arg].equals("-soft")) {
			job.setBoolean("soft",true);
			arg++;
		}

		String outputDir = args[arg];
		arg++;

		job.set("outputDir",outputDir);
		FileOutputFormat.setOutputPath(job, new Path (outputDir));

		boolean atLeastOneInput = false;
		for (int i = arg;i < args.length; i++) {
			FileSystem inputfs = FileSystem.get( new java.net.URI( args[i] ), getConf() );
			for ( FileStatus status : inputfs.globStatus( new Path( args[i] ) ) ) {
				Path inputPath  = status.getPath();
				atLeastOneInput = true;
				LOG.info( "Add input path: " + inputPath );
				FileInputFormat.addInputPath( job, inputPath );
			}
		}
		if (! atLeastOneInput) {
			LOG.info( "No input files to CDXGenerator." );
			return 0;
		}

		// Run the job!
		RunningJob rj = JobClient.runJob( job );
		if ( ! rj.isSuccessful( ) ) {
			LOG.error( "FAILED: " + rj.getID() );
			return 2;
		}
		return 0;
	}

	/**
	* Emit usage information for command-line driver.
	*/
	public void usage( ) {
		String usage =  "Usage: CDXGenerator [OPTIONS] <outputDir> <(w)arcfile>...\n" ;
		usage+=  "Options: -soft (keep job running despite some failures)\n" ;
		System.out.println( usage );
	}

	/**
	* Command-line driver.  Runs the CDXGenerator as a Hadoop job.
	*/
	public static void main( String args[] ) throws Exception {
		int result = ToolRunner.run(new Configuration(), new CDXGenerator(), args);
		System.exit( result );
	}

}
