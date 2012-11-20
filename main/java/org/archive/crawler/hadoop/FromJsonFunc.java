/**
 * 
 */
package org.archive.crawler.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.mortbay.util.ajax.JSON;

/**
 * A PIG function that translates JSON to Map object.
 * 
 * @author kenji
 */
public class FromJsonFunc extends EvalFunc<Map<String, Object>> {

  public FromJsonFunc() {
  }
  
  /* (non-Javadoc)
   * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> exec(Tuple input) throws IOException {
    if (input.size() != 1)
      throw new IllegalArgumentException("expects exactly one String argument");
    String value = (String)input.get(0);
    try {
      Object obj = JSON.parse(value);
      if (obj instanceof Map) {
	Map<?, ?> asmap = (Map<?, ?>)obj;
	for (Object k : asmap.keySet()) {
	  if (!(k instanceof String)) {
	    asmap.remove(k);
	  }
	}
	return (Map<String, Object>)asmap;
      } else {
	throw new IOException("JSON '" + value + "' cannot be parsed into a Map");
      }
    } catch (IllegalStateException ex) {
      // JSON.parse() throws IllegalStateException for syntactical errors.
      // translate it into IOException.
      throw new IOException("failed to parse as JSON:" + value);
    }
  }

}
