package com.puresoltechnologies.ductiledb.jdbc;

import org.junit.BeforeClass;

public abstract class AbstractJDBCTest {

    @BeforeClass
    public static void initialization() throws ClassNotFoundException {
	Class.forName(DuctileDriver.class.getName());
    }

}
