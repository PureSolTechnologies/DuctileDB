package com.puresoltechnologies.ductiledb.core;

import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

/**
 * This class creates the configuration for the tests.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBTestConfiguration {

    public static Map<String, String> createConfiguration() {
	Map<String, String> configuration = new HashMap<>();
	configuration.put(OSStorage.DIRECTORY_PROPERTY, "/tmp/ductiledb_test");
	return configuration;
    }

}
