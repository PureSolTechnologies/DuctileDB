package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractDuctileDBTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractDuctileDBTest.class);

    @BeforeClass
    public static void removeTables() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {
	    logger.info("Remove all DuctileDB tables...");
	    Connection connection = ((DuctileDBGraphImpl) graph).getConnection();
	    Admin admin = connection.getAdmin();
	    removeTable(admin, DuctileDBGraphImpl.METADATA_TABLE_NAME);
	    removeTable(admin, DuctileDBGraphImpl.VERTICES_TABLE_NAME);
	    removeTable(admin, DuctileDBGraphImpl.EDGES_TABLE_NAME);
	    removeTable(admin, DuctileDBGraphImpl.LABELS_TABLE_NAME);
	    removeTable(admin, DuctileDBGraphImpl.PROPERTIES_TABLE_NAME);
	    admin.deleteNamespace(DuctileDBGraphImpl.NAMESPACE_NAME);
	    logger.info("All DuctileDB tables removed.");
	}
    }

    private static void removeTable(Admin admin, String tableName) throws IOException {
	TableName vertexTableName = TableName.valueOf(tableName);
	admin.disableTable(vertexTableName);
	admin.deleteTable(vertexTableName);
    }
}
