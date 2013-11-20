package org.archive.cassandra;

import org.apache.cassandra.dht.*;
import java.nio.ByteBuffer;

public class BOP extends AbstractByteOrderedPartitioner
{
    public BytesToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;
        return new BytesToken(key);
    }
}
