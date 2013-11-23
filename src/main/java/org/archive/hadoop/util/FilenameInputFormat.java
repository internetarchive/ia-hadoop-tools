/*
 * Copyright 2012 Internet Archive
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
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Weird hack to take a filename of a file in HDFS and return that
 * name as the 1 and only 1 record "read" from it.
 */
public class FilenameInputFormat extends FileInputFormat<Text,Text>
{
  /**
   * Configure per Hadoop properties
   */
  public void configure( JobConf conf )
  {
  }

  /**
   * By definition, not splitable.
   */
  @Override
  protected boolean isSplitable(FileSystem fs, Path file) 
  {
    return false;
  }

  /**
   * Return a RecordReader which returns 1 record: the file path from
   * the InputSplit.
   */
  public RecordReader<Text, Text> getRecordReader( InputSplit genericSplit, 
                                                   JobConf job,
                                                   Reporter reporter)
    throws IOException 
    {
      reporter.setStatus(genericSplit.toString());
      
      FileSplit split = (FileSplit) genericSplit;
      final Path file  = split.getPath();
      
      return new RecordReader<Text,Text>()
        {
          boolean done = false; 

          public void close() 
          { 
          }
          
          public Text createKey() 
          {
            return new Text();
          }

          public Text createValue() 
          { 
            return new Text();
          }

          public long getPos() 
          { 
            return 0;
          }
          
          public float getProgress() 
          { 
            return 0.0f;
          }
          
          public boolean next( Text key, Text value) 
          { 
            if ( done ) return false;

            key.set( file.toString() );
            value.set( file.toString() );

            done = true ;

            return true;
          }

        };
    }
  
}
