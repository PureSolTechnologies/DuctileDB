package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

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
     * @throws IOException
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public static DuctileDBConfiguration readConfiguration(InputStream inputStream) throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	return objectMapper.readValue(inputStream, DuctileDBConfiguration.class);
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
