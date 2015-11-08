package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
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
	try (Connection connection = GraphFactory.createConnection(new BaseConfiguration())) {
	    logger.info("Remove all DuctileDB tables...");
	    Admin admin = connection.getAdmin();
	    HTableDescriptor[] listTables = admin.listTables();
	    for (HTableDescriptor tableDescriptor : listTables) {

		TableName tableName = tableDescriptor.getTableName();
		if (DuctileDBGraphImpl.DUCTILEDB_NAMESPACE.equals(tableName.getNamespaceAsString())) {
		    removeTable(admin, tableName);
		}
	    }
	    NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
	    for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
		if (DuctileDBGraphImpl.DUCTILEDB_NAMESPACE.equals(namespaceDescriptor.getName())) {
		    admin.deleteNamespace(DuctileDBGraphImpl.DUCTILEDB_NAMESPACE);
		}
	    }
	    logger.info("All DuctileDB tables removed.");
	}
    }

    private static void removeTable(Admin admin, TableName tableName) throws IOException {
	admin.disableTable(tableName);
	admin.deleteTable(tableName);
    }
}
