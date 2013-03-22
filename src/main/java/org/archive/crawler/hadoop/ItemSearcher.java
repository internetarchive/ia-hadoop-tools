/**
 * 
 */
package org.archive.crawler.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

/**
 * @author kenji
 *
 */
public interface ItemSearcher {
  /**
   * perform initialization. called when PetaboxFileSystem is initialized.
   * @param fsUri TODO
   * @param conf Hadoop Configuration object. shall have all PtaboxFileSystem
   * 	configuration parameters properly populated.
   */
  public void initialize(PetaboxFileSystem fs, URI fsUri, Configuration conf);
  /**
   * return FileStatus for each item belonging to a collection {@code collid}. 
   * @param collid collection identifier.
   * @return array of {@link FileStatus}
   */
  public FileStatus[] searchItems(String collid) throws IOException;
}
