package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * This is the central factory to connect to DuctileDB. The primary goal is to
 * have a simple point of access, because DuctileDB needs some information to
 * connect to Hadoop and HBase.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBBootstrap {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBBootstrap.class);
    private static DuctileDBImpl instance = null;

    private static Connection connection;

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
     * @throws SQLException
     */
    public synchronized static void start(DuctileDBConfiguration configuration) throws SQLException {
	if (instance == null) {
	    logger.info("Initializing PostgreSQL driver v" + org.postgresql.Driver.getVersion() + ".");
	    connection = DriverManager.getConnection(configuration.getDatabase().getJdbcUrl());
	    instance = new DuctileDBImpl(configuration, connection);
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
