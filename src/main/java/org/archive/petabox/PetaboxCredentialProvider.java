/**
 * 
 */
package org.archive.petabox;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpMessage;

/**
 * PetaoxAuthProvider that authenticate requests with two cookies: {@code logged-in-user}
 * and {@code logged-in-sig}.
 * changed from interface to abstract class as it is replaced by more general interface
 * {@link PetaboxAuthProvider}.
 * @author Kenji Nagahashi
 *
 */
public abstract class PetaboxCredentialProvider implements PetaboxAuthProvider {
    private static final Log LOG = LogFactory.getLog(PetaboxCredentialProvider.class);

  /**
   * value to be sent in logged-in-user cookie.
   * @return
   */
  public abstract String getUser();
  /**
   * value to be sent in logged-in-sig cookie.
   * @return
   */
  public abstract String getSignature();
  
  public void addAuthCookies(HttpMessage msg) {
      StringBuilder value = new StringBuilder();
      String user = getUser();
      if (user != null) {
          value.append("logged-in-user=").append(user).append("; ");
          LOG.debug("logged-in-user=" + user);
      }
      String sig = getSignature();
      if (sig != null) {
          value.append("logged-in-sig=").append(sig);
          LOG.debug("logged-in-sig=" + sig);
      }
      LOG.debug("adding auth cookies:" + value.toString());
      msg.addHeader("Cookie", value.toString());
  }
}
