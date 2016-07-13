package com.puresoltechnologies.ductiledb.core.graph;

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
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;

public class DuctileDBGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBGraphFactory.class);

    public static final String HADOOP_HOST_PROPERTY = "hadoop.host";
    public static final String HADOOP_PORT_PROPERTY = "hadoop.port";
    public static final String ZOOKEEPER_HOST_PROPERTY = "zookeeper.host";
    public static final String ZOOKEEPER_PORT_PROPERTY = "zookeeper.port";
    public static final String HBASE_MASTER_HOST_PROPERTY = "hbase.master.host";
    public static final String HBASE_MASTER_PORT_PROPERTY = "hbase.master.port";

    public static final int DEFAULT_MASTER_PORT = 60000;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;

    public static Configuration createConfiguration(File hbaseSiteFile) {
	Configuration hbaseConfiguration = HBaseConfiguration.create();
	hbaseConfiguration.addResource(new Path(hbaseSiteFile.getPath()));
	return hbaseConfiguration;
    }

    public static Configuration createConfiguration(String zooKeeperHost, int zookeeperPort, String masterHost,
	    int masterPort) {
	Configuration config = HBaseConfiguration.create();
	config.clear();
	config.set("hbase.zookeeper.quorum", zooKeeperHost);
	config.set("hbase.zookeeper.property.clientPort", String.valueOf(zookeeperPort));
	config.set("hbase.master", masterHost + ":" + masterPort);
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

    public static Connection createConnection(Configuration configuration) throws IOException, ServiceException {
	// TODO incorporate configuration...
	logger.info("Creating connection to HBase with configuration '" + configuration + "'...");
	HBaseAdmin.checkHBaseAvailable(configuration);
	Connection connection = ConnectionFactory.createConnection(configuration);
	logger.info("Connection '" + connection + "' to HBase created.");
	return connection;
    }

    public static DuctileDBGraph createGraph(BlobStoreImpl blobStore, Configuration hbaseConfiguration)
	    throws IOException, ServiceException {
	Connection connection = createConnection(hbaseConfiguration);
	return new DuctileDBGraphImpl(blobStore, connection, true);
    }

    public static DuctileDBGraph createGraph(BlobStoreImpl blobStore, String zookeeperHost, int zookeeperPort,
	    String masterHost, int masterPort)
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	Configuration configuration = createConfiguration(zookeeperHost, zookeeperPort, masterHost, masterPort);
	return createGraph(blobStore, configuration);
    }

}
