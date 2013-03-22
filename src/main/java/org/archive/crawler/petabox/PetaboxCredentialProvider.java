/**
 * 
 */
package org.archive.crawler.petabox;

/**
 * a component that provides credential for accessing petabox
 * @author Kenji Nagahashi
 *
 */
public interface PetaboxCredentialProvider {
  /**
   * value to be sent in logged-in-user cookie.
   * @return
   */
  public String getUser();
  /**
   * value to be sent in logged-in-sig cookie.
   * @return
   */
  public String getSignature();
}
