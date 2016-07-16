package com.puresoltechnologies.ductiledb.core.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.commons.misc.hash.HashId;
import com.puresoltechnologies.commons.misc.hash.HashUtilities;
import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.storage.engine.utils.IOUtils;

public class BlobStoreIT extends AbstractDuctileDBTest {

    private static DuctileDB ductileDB;
    private static BlobStore blobStore;

    @BeforeClass
    public static void initialize() {
	ductileDB = getDuctileDB();
	blobStore = ductileDB.getBlobStore();
    }

    @Test
    public void testCreateReadDelete() throws IOException {
	String blobContent = "I am a BLOB content. :-)";
	HashId hashId = HashUtilities.createHashId(blobContent);
	assertFalse("BLOB should not exist.", blobStore.isBlobAvailable(hashId));

	ByteArrayInputStream inputStream = new ByteArrayInputStream(blobContent.getBytes(Charset.defaultCharset()));
	blobStore.storeBlob(hashId, inputStream);
	assertTrue("BLOB should exist.", blobStore.isBlobAvailable(hashId));

	InputStream readBlobStream = blobStore.readBlob(hashId);
	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	IOUtils.copy(readBlobStream, outputStream);
	assertEquals("Read BLOB should match stored BLOB.", blobContent,
		new String(outputStream.toByteArray(), Charset.defaultCharset()));

	long size = blobStore.getBlobSize(hashId);
	assertEquals("Size of BLOB should match original.", blobContent.getBytes(Charset.defaultCharset()).length,
		size);

	blobStore.removeBlob(hashId);
	assertFalse("BLOB should not exist anymore.", blobStore.isBlobAvailable(hashId));
    }
}
