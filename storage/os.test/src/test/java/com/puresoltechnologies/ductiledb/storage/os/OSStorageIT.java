package com.puresoltechnologies.ductiledb.storage.os;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;
import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public class OSStorageIT {

    private static StorageConfiguration configuration = new StorageConfiguration();;

    @Rule
    public ExpectedException exception = ExpectedException.none();

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

}
