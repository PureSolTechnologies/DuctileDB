package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.GraphFactory;
import com.puresoltechnologies.ductiledb.HGraph;
import com.puresoltechnologies.ductiledb.HGraphImpl;

public class AbstractHGraphTest {

    @BeforeClass
    public static void removeTables() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    Connection connection = ((HGraphImpl) graph).getConnection();
	    Admin admin = connection.getAdmin();
	    TableName vertexTableName = TableName.valueOf(HGraphImpl.VERTICES_TABLE_NAME);
	    admin.disableTable(vertexTableName);
	    admin.deleteTable(vertexTableName);
	    admin.deleteNamespace(HGraphImpl.NAMESPACE_NAME);
	}
    }

}
