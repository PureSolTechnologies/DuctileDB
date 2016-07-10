package com.puresoltechnologies.ductiledb.core;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;

public class DuctileDBGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBGraphFactory.class);

    public static final String ZOOKEEPER_HOST_PROPERTY = "zookeeper.host";
    public static final String ZOOKEEPER_PORT_PROPERTY = "zookeeper.port";
    public static final String HBASE_MASTER_HOST_PROPERTY = "hbase.master.host";
    public static final String HBASE_MASTER_PORT_PROPERTY = "hbase.master.port";

    public static final int DEFAULT_MASTER_PORT = 60000;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;

    public static Configuration createConfiguration(File hbaseSiteFile)
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	Configuration hbaseConfiguration = HBaseConfiguration.create();
	hbaseConfiguration.addResource(new Path(hbaseSiteFile.getPath()));
	HBaseAdmin.checkHBaseAvailable(hbaseConfiguration);
	return hbaseConfiguration;
    }

    public static Configuration createConfiguration(String zooKeeperHost, int zookeeperPort, String masterHost,
	    int masterPort)
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	Configuration config = HBaseConfiguration.create();
	config.clear();
	config.set("hbase.zookeeper.quorum", zooKeeperHost);
	config.set("hbase.zookeeper.property.clientPort", String.valueOf(zookeeperPort));
	config.set("hbase.master", masterHost + ":" + masterPort);
	HBaseAdmin.checkHBaseAvailable(config);
	return config;
    }

    public static Connection createConnection(File hbaseSiteFile) throws IOException, ServiceException {
	Configuration hbaseConfiguration = createConfiguration(hbaseSiteFile);
	return createConnection(hbaseConfiguration);
    }

    public static Connection createConnection(String zooKeeperHost, int zookeeperPort, String masterHost,
	    int masterPort)
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	Configuration configuration = createConfiguration(zooKeeperHost, zookeeperPort, masterHost, masterPort);
	return createConnection(configuration);
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

    public static DuctileDBGraph createGraph(File hbaseSiteFile) throws IOException, ServiceException {
	Connection connection = createConnection(hbaseSiteFile);
	return createGraph(connection, true);
    }

    public static DuctileDBGraph createGraph(String zookeeperHost, int zookeeperPort, String masterHost, int masterPort)
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	Configuration configuration = createConfiguration(zookeeperHost, zookeeperPort, masterHost, masterPort);
	return createGraph(configuration);
    }

    public static DuctileDBGraph createGraph(Connection connection) throws IOException {
	return createGraph(connection, false);
    }

    public static DuctileDBGraph createGraph(Connection connection, boolean closeConnetion) throws IOException {
	return new DuctileDBGraphImpl(connection, closeConnetion);
    }

}
