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
 * A PIG function that generates JSON from Map object.
 * 
 * @author kenji
 */
public class ToJsonFunc extends EvalFunc<String> {

  public ToJsonFunc() {
  }
  
  /* (non-Javadoc)
   * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
   */
  @SuppressWarnings("unchecked")
  @Override
  public String exec(Tuple input) throws IOException {
    if (input.size() != 1)
      throw new IllegalArgumentException("expects exactly one Map argument");
    Map<String, Object> value = (Map<String, Object>)input.get(0);
    String json = value != null ? JSON.toString(value) : null;
    return json;
  }

}
