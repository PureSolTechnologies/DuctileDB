package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.junit.Test;

public class DuctileConnectionIT extends AbstractJDBCTest {

    @Test
    public void testGenerals() throws SQLException {
	DuctileConnection connection = getConnection();
	assertNotNull(connection.getClientInfo());
    }

}
