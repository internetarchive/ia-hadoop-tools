package org.archive.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.jets3t.service.S3ServiceException;

public class IAS3 extends NativeS3FileSystem {

	// Suppress S3ServiceException
	@Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		boolean result = false;
		
		try {
			result = super.mkdirs(f, permission);
		} catch (IOException io) {
			if (io.getCause() instanceof S3ServiceException) {
				result = true;
			}
		}
		
		return result;
	}
}
