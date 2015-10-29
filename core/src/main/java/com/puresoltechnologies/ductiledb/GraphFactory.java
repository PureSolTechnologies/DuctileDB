package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(GraphFactory.class);

    public static DuctileDBGraph createGraph() throws IOException {
	Configuration hbaseConfiguration = HBaseConfiguration.create();
	hbaseConfiguration.addResource(new Path("/opt/hbase/conf/hbase-site.xml"));
	logger.info("Creating connection to HBase with configuration '" + hbaseConfiguration + "'...");
	Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
	logger.info("Connection to HBase created.");
	return new DuctileDBGraphImpl(connection);
    }

}
