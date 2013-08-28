/**
 * 
 */
package org.archive.petabox;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpMessage;
import org.archive.util.HMACSigner;

/**
 * Authenticates petabox requests by HMAC.
 * Commonly used for gaining read-only access to restricted items.
 * 
 * @author kenji
 *
 */
public class HMACPetaboxAuthProvider implements PetaboxAuthProvider {
    private static final Log LOG = LogFactory.getLog(HMACPetaboxAuthProvider.class);
    
    protected String secretKey;
    protected String name = "webdata";
    protected long expiration = 30;
    private String lastDigest = null;
    private long lastGeneration = 0;
    
    /**
     * set secret key for computing a hash of message.
     * @param secretKey
     */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
    
    @Override
    public void addAuthCookies(HttpMessage msg) {
        if (lastDigest == null || System.currentTimeMillis() > lastGeneration + expiration * 1000 / 2) {
            HMACSigner signer = new HMACSigner(secretKey, name);
            lastDigest = signer.getHMacCookieStr(null, expiration);
            lastGeneration = System.currentTimeMillis();
        }
        //LOG.info("Cookie: " + lastDigest);
        msg.addHeader("Cookie", lastDigest);
    }
    
}
