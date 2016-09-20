package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.api.DuctileDB;

/**
 * This is the central factory to connect to DuctileDB. The primary goal is to
 * have a simple point of access, because DuctileDB needs some information to
 * connect to Hadoop and HBase.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBBootstrap {

    private static DuctileDBImpl instance = null;

    public static DuctileDBConfiguration readConfiguration(URL configurationFile) throws IOException {
	try (InputStream fileInputStream = configurationFile.openStream()) {
	    return readConfiguration(fileInputStream);
	}
    }

    /**
     * Reads the Yaml configuration out of the provided input stream.
     * 
     * @param inputStream
     * @return
     */
    public static DuctileDBConfiguration readConfiguration(InputStream inputStream) {
	Yaml yaml = new Yaml();
	return yaml.loadAs(inputStream, DuctileDBConfiguration.class);
    }

    /**
     * Starts the database with the provided configuration.
     * 
     * @param configuration
     *            is the configuration to be used to create the acutal instance.
     */
    public synchronized static void start(DuctileDBConfiguration configuration) {
	if (instance == null) {
	    instance = new DuctileDBImpl(configuration);
	}
    }

    /**
     * Stops the database.
     */
    public synchronized static void stop() {
	if (instance != null) {
	    instance.close();
	    instance = null;
	}
    }

    public static DuctileDB getInstance() {
	return instance;
    }
}
