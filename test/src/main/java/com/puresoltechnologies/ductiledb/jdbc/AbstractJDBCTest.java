package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;

public abstract class AbstractJDBCTest extends AbstractDuctileDBTest {

    @BeforeClass
    public static void initialization() throws ClassNotFoundException {
	Class.forName(DuctileDriver.class.getName());
    }

    private Connection connection = null;

    @Before
    public void connect() throws SQLException {
	assertNull(connection);
	Connection connection = DriverManager.getConnection(
		"jdbc:ductile:file:" + new File("src/test/resources/ductiledb-test.yml").getAbsolutePath());
	assertEquals(DuctileConnection.class, connection.getClass());
    }

    @After
    public void disconnect() throws SQLException {
	assertNotNull(connection);
	connection.close();
    }

    protected Connection getConnection() {
	return connection;
    }

}
