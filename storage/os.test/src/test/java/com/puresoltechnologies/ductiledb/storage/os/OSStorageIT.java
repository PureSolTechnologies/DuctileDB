package com.puresoltechnologies.ductiledb.storage.os;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;
import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public class OSStorageIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private StorageConfiguration configuration;

    @Before
    public void createConfiguration() {
	configuration = new StorageConfiguration();
    }

    @Test
    public void testEmptyDirectory() throws IOException {
	exception.expect(IllegalArgumentException.class);
	exception.expectMessage("Directory was not set via property '" + OSStorage.DIRECTORY_PROPERTY + "'.");
	try (OSStorage storage = new OSStorage(configuration)) {

	}
    }

    @Test
    public void testIllegalDirectory() throws IOException {
	exception.expect(IOException.class);
	exception.expectMessage("Could not create directory '/test'.");
	configuration.getProperties().setProperty(OSStorage.DIRECTORY_PROPERTY, "/test");
	try (OSStorage storage = new OSStorage(configuration)) {
	    storage.initialize();
	}
    }

    @Test
    public void testDeletionAfterLastStreamClose() throws IOException, InterruptedException {
	configuration.getProperties().setProperty(OSStorage.DIRECTORY_PROPERTY, "/tmp/test");
	configuration.getProperties().setProperty(OSStorage.DELETION_PERIOD_PROPERTY, "1000");
	try (OSStorage storage = new OSStorage(configuration)) {
	    storage.initialize();

	    File storageDirectory = storage.getStorageDirectory();
	    String fileName = "deletion.test";
	    File file = new File(storageDirectory, fileName);
	    if (file.exists()) {
		assertTrue(file.delete());
	    }
	    assertFalse(file.exists());
	    try (BufferedOutputStream created = storage.create(new File("/" + fileName))) {
		assertTrue(file.exists());
	    }
	    storage.delete(new File("/" + fileName));
	    TimeUnit.MILLISECONDS.sleep(1500);
	    assertFalse(file.exists());

	    try (BufferedOutputStream created = storage.create(new File("/" + fileName))) {
		assertTrue(file.exists());
	    }
	    try (BufferedInputStream opened = storage.open(new File("/" + fileName))) {
		assertTrue(file.exists());
		storage.delete(new File("/" + fileName));
		assertTrue(file.exists());
		TimeUnit.MILLISECONDS.sleep(1500);
		assertTrue(file.exists());
	    }
	    storage.delete(new File("/" + fileName));
	    TimeUnit.MILLISECONDS.sleep(1500);
	    assertFalse(file.exists());
	}

    }
}
