package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;

import org.junit.BeforeClass;

public class AbstractDuctileDBTest {

    @BeforeClass
    public static void removeTables() throws IOException {
	DuctileDBTestHelper.removeTables();
    }
}
