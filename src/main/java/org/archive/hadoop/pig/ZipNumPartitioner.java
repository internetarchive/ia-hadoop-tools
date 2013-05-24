package org.archive.hadoop.pig;

import org.apache.pig.impl.io.NullableText;

public class ZipNumPartitioner extends org.archive.hadoop.mapreduce.ZipNumPartitioner<NullableText, NullableText> {

	@Override
	public int getPartition(NullableText key, NullableText value, int numSplits) {

		return super.getPartition(key.getValueAsPigType().toString(), numSplits);
	}
}
