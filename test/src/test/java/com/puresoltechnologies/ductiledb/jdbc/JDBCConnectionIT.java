package com.puresoltechnologies.ductiledb.jdbc;

import java.sql.Connection;

import org.junit.Test;

public class JDBCConnectionIT extends AbstractJDBCTest {

    @Test
    public void test() {
	Connection connection = getConnection();
    }

}
