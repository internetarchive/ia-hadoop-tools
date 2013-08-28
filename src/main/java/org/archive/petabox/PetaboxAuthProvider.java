/**
 * 
 */
package org.archive.petabox;

import org.apache.http.HttpMessage;

/**
 * PetaboxAuthProvider configures HTTP request for authentication.
 * 
 * @author kenji
 *
 */
public interface PetaboxAuthProvider {
    public void addAuthCookies(HttpMessage msg);
}
