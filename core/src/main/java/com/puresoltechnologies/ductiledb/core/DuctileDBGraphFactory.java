package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;

public class DuctileDBGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBGraphFactory.class);

    public static Configuration createDefaultConfiguration() {
	Configuration hbaseConfiguration = HBaseConfiguration.create();
	hbaseConfiguration.addResource(new Path("/opt/hbase/conf/hbase-site.xml"));
	return hbaseConfiguration;
    }

    public static Connection createConnection(org.apache.commons.configuration.Configuration configuration)
	    throws IOException {
	// TODO incorporate configuration...
	Configuration hbaseConfiguration = createDefaultConfiguration();
	return createConnection(hbaseConfiguration);
    }

    public static Connection createConnection(Configuration hbaseConfiguration) throws IOException {
	// TODO incorporate configuration...
	logger.info("Creating connection to HBase with configuration '" + hbaseConfiguration + "'...");
	Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
	logger.info("Connection '" + connection + "' to HBase created.");
	return connection;
    }

    public static DuctileDBGraph createGraph(Configuration hbaseConfiguration) throws IOException {
	Connection connection = createConnection(hbaseConfiguration);
	return createGraph(connection, true);
    }

    public static DuctileDBGraph createGraph(org.apache.commons.configuration.Configuration configuration)
	    throws IOException {
	Connection connection = createConnection(configuration);
	return createGraph(connection, true);
    }

    public static DuctileDBGraph createGraph(Connection connection) throws IOException {
	return createGraph(connection, false);
    }

    public static DuctileDBGraph createGraph(Connection connection, boolean closeConnetion) throws IOException {
	return new DuctileDBGraphImpl(connection, closeConnetion);
    }

}
