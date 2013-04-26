/**
 * 
 */
package org.archive.petabox;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * class under test: {@link ItemMetadata}
 * @author kenji
 *
 */
public class ItemMetadataTest extends Assert {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testRegularInputThrougReader() throws IOException {
		Reader r = new InputStreamReader(getClass().getResourceAsStream("metadata-regular.json"));
		ItemMetadata md = new ItemMetadata(r);
		
		assertEquals("server", "ia701201.us.archive.org", md.getServer());
		assertEquals("d1", "ia601201.us.archive.org", md.getD1());
		assertEquals("d2", "ia701201.us.archive.org", md.getD2());
		assertEquals("isCollection", true, md.isCollection());
		assertEquals("isSolo", false, md.isSolo());
		
		assertEquals("files.length", 5, md.getFiles().length);
	}

}
