/**
 * 
 */
package org.archive.petabox;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * reads petabox credential from a file in Mozilla cookie format.
 * by default it reads ~/.iaauth. specify File or call {@link #setCookieFile(File)} to
 * read cookies from different file. 
 * @author Kenji Nagahashi
 *
 */
public class CookieFilePetaboxCredentialProvider implements
    PetaboxCredentialProvider {
  private static final Log LOG = LogFactory.getLog(CookieFilePetaboxCredentialProvider.class);
  
  private static final String DEFAULT_COOKIE_FILENAME = ".iaauth";
  
  private File cookieFile = null;
  private String cachedUser;
  private String cachedSignature;
  
  private File getDefautlFile() {
    String home = System.getProperty("user.home");
    if (home == null) return null;
    File f = new File(home, DEFAULT_COOKIE_FILENAME);
    return f;
  }
  public CookieFilePetaboxCredentialProvider() {
  }
  public CookieFilePetaboxCredentialProvider(File cookieFile) {
    this.cookieFile = cookieFile;
  }
  
  public File getCookieFile() {
    return cookieFile;
  }
  public void setCookieFile(File cookieFile) {
    this.cookieFile = cookieFile;
    clearCached();
  }
  protected void readCredential() {
    File cf = cookieFile != null ? cookieFile : getDefautlFile();
    if (!cf.canRead()) return;
    try {
      Reader r = new FileReader(cf);
      BufferedReader br = new BufferedReader(r);
      try {
	String line;
	while ((line = br.readLine()) != null) {
	  if (line.startsWith("#")) continue;
	  String[] fields = line.split("\\s+");
	  if (fields.length < 7) continue;
	  // XXX this menas this class works only against archive.org
	  if (!fields[0].equals(".archive.org")) continue;
	  if (fields[5].equals("logged-in-user")) {
	    this.cachedUser = fields[6];
	  } else if (fields[5].equals("logged-in-sig")) {
	    this.cachedSignature = fields[6];
	  }
	}
      } finally {
	br.close();
      }
    } catch (IOException ex) {
      LOG.warn("error reading "+cf.getPath(), ex);
    }
  }
  protected void clearCached() {
    cachedUser = cachedSignature = null;
  }
  /* (non-Javadoc)
   * @see org.archive.crawler.petabox.PetaboxCredentialProvider#getSignature()
   */
  @Override
  public String getSignature() {
    if (cachedSignature == null)
      readCredential();
    return cachedSignature;
  }

  /* (non-Javadoc)
   * @see org.archive.crawler.petabox.PetaboxCredentialProvider#getUser()
   */
  @Override
  public String getUser() {
    if (cachedUser == null)
      readCredential();
    return cachedUser;
  }

}
