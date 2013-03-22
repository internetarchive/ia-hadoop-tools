/**
 * 
 */
package org.archive.crawler.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


/**
 * NB: this test case currently requires live access to archive.org.
 * 
 * @author kenji
 *
 */
public class PetaboxFileSystemTest {
  Configuration conf;
  PetaboxFileSystem pfs;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    pfs = new PetaboxFileSystem();
    pfs.initialize(new URI("petabox://archive.org/"), conf);
  }
  
  @Test
  public void testProperties() {
    assertEquals("petabox://archive.org/", pfs.getUri().toString());
  }
  
  // milliseconds timestamp for 1971-01-01 00:00:00 for testing mtime
  final static long LIKELY_BE_SECONDS = 31536000000L;
  
  @Test
  public void testGetFileStatus_ForItem() throws Exception {
    // item known to exist
    final String path1 = "/wide00006";
    final String path2 = "petabox://archive.org/wide00006";
    
    FileStatus fst1 = pfs.getFileStatus(new Path(path1));
    assertNotNull(fst1);
    assertEquals("item is a directory", true, fst1.isDir());
    assertTrue("replication > 0", fst1.getReplication() > 0);
    assertEquals("absolute path matches", path2, fst1.getPath().toString());
    
    FileStatus fst2 = pfs.getFileStatus(new Path(path2));
    assertNotNull(fst2);
    assertEquals("item is a directory", true, fst2.isDir());
    assertTrue("replication > 0", fst2.getReplication() > 0);
    assertEquals("path matches the orginal", path2, fst2.getPath().toString());
    
    // modification time is in milliseconds. this is not perfect, but should catch mtime in
    // seconds.
    assertTrue("modification time " + fst2.getModificationTime() + " is big enough as ms",
	fst2.getModificationTime() > LIKELY_BE_SECONDS);
  }
  
  @Test
  public void testGetFileStatus_ForFile() throws Exception {
    // item known to exist
    final String path1 = "/WIDE-20120914205820-crawl410/WIDE-20120914205820-crawl410.cdx.gz";
    final String path2 = "petabox://archive.org/WIDE-20120914205820-crawl410/WIDE-20120914205820-crawl410.cdx.gz";
    
    FileStatus fst1 = pfs.getFileStatus(new Path(path1));
    assertNotNull(fst1);
    assertEquals("path is not a directory", false, fst1.isDir());
    assertTrue("replication > 0", fst1.getReplication() > 0);
    assertEquals("absolute path matches", path2, fst1.getPath().toString());
    
    FileStatus fst2 = pfs.getFileStatus(new Path(path2));
    assertNotNull(fst2);
    assertEquals("path is not a directory", false, fst2.isDir());
    assertTrue("replication > 0", fst2.getReplication() > 0);
    assertEquals("path matches the orginal", path2, fst2.getPath().toString());
    
    // modification time is in milliseconds. this is not perfect, but should catch mtime in
    // seconds.
    assertTrue("modification time " + fst2.getModificationTime() + " is big enough as ms", 
	fst2.getModificationTime() > LIKELY_BE_SECONDS);
  }
}
