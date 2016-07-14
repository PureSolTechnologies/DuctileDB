package com.puresoltechnologies.ductiledb.storage.os;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public class OSStorageIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testEmptyDirectory() {
	exception.expect(IllegalArgumentException.class);
	exception.expectMessage("Directory was not set via property '" + OSStorage.DIRECTORY_PROPERTY + "'.");
	Map<String, String> configuration = new HashMap<>();
	new OSStorage(configuration);
    }

    @Test
    public void testIllegalDirectory() throws IOException {
	exception.expect(IOException.class);
	exception.expectMessage("Could not create directory '/test'.");
	Map<String, String> configuration = new HashMap<>();
	configuration.put(OSStorage.DIRECTORY_PROPERTY, "/test");
	new OSStorage(configuration).initialize();
    }

}
