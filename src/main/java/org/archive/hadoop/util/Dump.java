/*
 * Copyright 2011 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.archive.hadoop.util;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Command-line utility to dump the contents of one or more Hadoop
 * MapFile or SequenceFile.  Default is to emit both key and value,
 * but "-k" or "-v" command-line options can select one or the other.
 */
public class Dump extends Configured implements Tool
{

  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(Dump.class), new Dump(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    String usage = "Usage: Dump [-k|-v] <mapfile|sequencefile>...";
      
    if ( args.length < 1 )
      {
        System.err.println(usage);
        return 1;
      }
    
    int i = 0;
    int mode = 0;
    if ( args[0].equals( "-k" ) )
      {
        mode = 1;
        i++;
      }
    else if ( args[0].equals( "-v" ) )
      {
        mode = 2;
        i++;
      }
    
    for ( ; i < args.length; i++ )
      {
        FileSystem inputfs = FileSystem.get( new java.net.URI( args[i] ), getConf() );
        
        for ( FileStatus status : inputfs.globStatus( new Path( args[i] ) ) )
          {
            Path inputPath  = status.getPath();
            
            dump( inputfs, inputPath, mode );
          }
      }

    return 0;
  }

  public void dump( FileSystem fs, Path inputPath, int mode ) throws Exception
  {
    Configuration conf = getConf();

    MapFile     .Reader mapReader = null;
    SequenceFile.Reader seqReader = null;
    try
      {
        mapReader = new MapFile.Reader( fs, inputPath.toString(), conf );
      }
    catch ( IOException ioe )
      {
        // Hrm, try a sequence file...
      }

    if ( mapReader != null )
      {
        WritableComparable key   = (WritableComparable) ReflectionUtils.newInstance(mapReader.getKeyClass()  , conf);
        Writable           value = (Writable)           ReflectionUtils.newInstance(mapReader.getValueClass(), conf);

        while ( mapReader.next(key, value))
          {
            output( key, value, mode );
          }
      }
    else
      {
        // Not a MapFile...try a SequenceFile.
        try
          {
            seqReader = new SequenceFile.Reader( fs, inputPath, conf );
          }
        catch ( IOException ioe )
          {
            // Hrm, neither MapFile nor SequenceFile.
            throw new IOException( "Cannot open file: " + inputPath );
          }

        WritableComparable key   = (WritableComparable) ReflectionUtils.newInstance(seqReader.getKeyClass()  , conf);
        Writable           value = (Writable)           ReflectionUtils.newInstance(seqReader.getValueClass(), conf);

        while ( seqReader.next(key, value))
          {
            output( key, value, mode );
          }
      }
  }

  void output( Writable key, Writable value, int mode )
  {
    switch ( mode )
      {
      case 0:
        System.out.print( "[" );
        System.out.print( key );
        System.out.print( "] [" );
        System.out.print( value );
        System.out.println( "]" );
        break;
        
      case 1:
        System.out.println( key );
        break;

      case 2:
        System.out.println( value );
        break;
      }
  }
}
