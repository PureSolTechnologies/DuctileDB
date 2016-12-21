package com.puresoltechnologies.ductiledb.blobstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

public class BlobStoreIT extends AbstractBlobStoreTest {

    @Test
    public void test() throws SQLException, IOException {
	BlobStore blobStore = new BlobStoreImpl(readBlobStoreConfiguration(), getConnection());
	assertEquals(0, blobStore.getBlobCount());
	assertEquals(0, blobStore.getBlobStoreSize());
    }

    @Test
    public void testGRUD() throws IOException, SQLException {
	BlobStore blobStore = new BlobStoreImpl(readBlobStoreConfiguration(), getConnection());
	File testFile = new File("src/test/resources/database.yml");
	assertTrue(testFile.exists());
	try (FileInputStream fileInputStream = new FileInputStream(testFile)) {
	    blobStore.storeBlob(fileInputStream);
	}
    }
}
