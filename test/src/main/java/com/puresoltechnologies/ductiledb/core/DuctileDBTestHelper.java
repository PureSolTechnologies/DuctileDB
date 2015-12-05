package com.puresoltechnologies.ductiledb.core;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.DUCTILEDB_NAMESPACE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;

/**
 * A collection of simple methods to support testing.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTestHelper {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBTestHelper.class);

    /**
     * Runs through an {@link Iterable} and counts the number of elements.
     * 
     * @param iterable
     *            is the {@link Iterable} to count the elements in.
     * @return
     */
    public static long count(Iterable<?> iterable) {
	long[] count = { 0 };
	iterable.forEach(c -> count[0]++);
	return count[0];
    }

    public static void removeTables() throws IOException {
	try (Connection connection = GraphFactory.createConnection(new BaseConfiguration())) {
	    removeTables(connection);
	}
    }

    public static void removeTables(Connection connection) throws IOException {
	logger.info("Remove all DuctileDB tables...");
	Admin admin = connection.getAdmin();
	HTableDescriptor[] listTables = admin.listTables();
	for (HTableDescriptor tableDescriptor : listTables) {
	    TableName tableName = tableDescriptor.getTableName();
	    if (DUCTILEDB_NAMESPACE.equals(tableName.getNamespaceAsString())) {
		removeTable(admin, tableName);
	    }
	}
	NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
	for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
	    if (DUCTILEDB_NAMESPACE.equals(namespaceDescriptor.getName())) {
		admin.deleteNamespace(DUCTILEDB_NAMESPACE);
	    }
	}
	logger.info("All DuctileDB tables removed.");
    }

    private static void removeTable(Admin admin, TableName tableName) throws IOException {
	if (admin.isTableEnabled(tableName)) {
	    logger.info("Disable table '" + tableName + "'...");
	    admin.disableTable(tableName);
	    logger.info("Table '" + tableName + "' disabled.");
	}
	logger.info("Delete table '" + tableName + "'...");
	admin.deleteTable(tableName);
	logger.info("Table '" + tableName + "' deleted.");
    }

    public static void removeGraph(DuctileDBGraph graph) throws IOException {
	for (DuctileDBVertex vertex : graph.getVertices()) {
	    vertex.remove();
	}
	graph.commit();
	assertEquals(DuctileDBGraphImpl.class, graph.getClass());
	new DuctileDBHealthCheck((DuctileDBGraphImpl) graph).runCheck();
    }
}
