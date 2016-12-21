package com.puresoltechnologies.ductiledb.backend;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;

import org.junit.Test;

public class PostgreConnectionPoolIT {

    @Test
    public void testPoolSettings() throws IOException {
	DatabaseConfiguration databaseConfiguration = AbstractBackendTest.readDatabaseConfiguration();
	ConnectionPool pool = new ConnectionPool(databaseConfiguration);
	assertEquals(10, pool.getMaxTotal());
	assertEquals(4, pool.getMinIdle());
	assertEquals(pool.getMaxTotal(), pool.getMaxIdle());
    }

    @Test
    public void testConnectionsInPool() throws Exception {
	DatabaseConfiguration databaseConfiguration = AbstractBackendTest.readDatabaseConfiguration();
	ConnectionPool pool = new ConnectionPool(databaseConfiguration);
	pool.clear();
	assertEquals(0, pool.getNumActive());
	assertEquals(0, pool.getNumIdle());
	Connection connection1 = pool.borrowObject();
	assertEquals(1, pool.getNumActive());
	assertEquals(0, pool.getNumIdle());
	Connection connection2 = pool.borrowObject();
	assertEquals(2, pool.getNumActive());
	assertEquals(0, pool.getNumIdle());
	pool.returnObject(connection1);
	assertEquals(1, pool.getNumActive());
	assertEquals(1, pool.getNumIdle());
	pool.returnObject(connection2);
	assertEquals(0, pool.getNumActive());
	assertEquals(2, pool.getNumIdle());
    }

}
