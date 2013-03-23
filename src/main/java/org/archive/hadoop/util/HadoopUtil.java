/**
 * 
 */
package org.archive.hadoop.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/**
 * @author kenji
 *
 */
public class HadoopUtil {
  // code stolen from org.apche.hadoop.hbase.mapreduce.TableMapReduceUtil.
  // too bad this is not part of Hadoop rather than Hbase. I just made it free of
  // generics-related warnings.
  static Log LOG = LogFactory.getLog(HadoopUtil.class);

  
  /**
   * Add the jars containing the given classes to the job's configuration
   * such that JobClient will ship them to the cluster and add them to
   * the DistributedCache.
   */
  public static void addDependencyJars(Configuration conf,
      Class<?>... classes) throws IOException {

    FileSystem localFs = FileSystem.getLocal(conf);

    Set<String> jars = new HashSet<String>();

    // Add jars that are already in the tmpjars variable
    jars.addAll( conf.getStringCollection("tmpjars") );

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null) continue;

      String pathStr = findContainingJar(clazz);
      if (pathStr == null) {
        LOG.warn("Could not find jar for class " + clazz +
                 " in order to ship it to the cluster.");
        continue;
      }
      Path path = new Path(pathStr);
      if (!localFs.exists(path)) {
        LOG.warn("Could not validate jar file " + path + " for class "
                 + clazz);
        continue;
      }
      jars.add(path.makeQualified(localFs).toString());
    }
    if (jars.isEmpty()) return;

    conf.set("tmpjars",
             StringUtils.arrayToString(jars.toArray(new String[0])));
  }

  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * This is shamelessly copied from JobConf
   * 
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration<URL> itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
