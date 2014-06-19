package org.archive.hadoop.pig.udf;

import java.io.IOException;

import org.apache.commons.lang.BooleanUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.archive.hadoop.mapreduce.CDXMapper;

public class Recanonicalize extends EvalFunc<String> {
	
	CDXMapper converter;
	
	public Recanonicalize()
	{
		this(true);
	}
	
	public Recanonicalize(String isSurt)
	{
		this(BooleanUtils.toBoolean(isSurt));
	}
	
	public Recanonicalize(boolean isSurt)
	{
		converter = new CDXMapper(isSurt);
		converter.setNoRedirect(true);
		converter.setSkipOnCanonFail(true);
	}

	@Override
	public String exec(Tuple tuple) throws IOException {
		
		if (tuple == null || tuple.size() == 0) {
			return null;
		}
		
		if (tuple.size() == 1) {
			String line = (String)tuple.get(0);
			// If only the url, then convert url instead of whole cdx line
			if (!line.contains(" ")) {
				return converter.canonicalizeUrl(line);
			} else {
				return converter.convertLine(line);	
			}
		} else if (tuple.size() == 2) {
			String key = (String)tuple.get(0);
			String value = (String)tuple.get(1);
			return converter.convertLine(key + " " + value);			
		} else {
			throw new IOException("CDX tuple must be length 1 or 2");
		}
	}
}
