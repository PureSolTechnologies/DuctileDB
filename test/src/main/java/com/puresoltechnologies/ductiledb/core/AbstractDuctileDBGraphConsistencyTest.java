package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;

public class AbstractDuctileDBGraphConsistencyTest extends AbstractDuctileDBGraphTest {

    protected static DuctileDBHealthCheck check;

    @BeforeClass
    public static void connect() throws IOException {
	check = new DuctileDBHealthCheck((DuctileDBGraphImpl) graph);
    }

    @AfterClass
    public static void disconnect() throws IOException {
	check.runCheck();
	check = null;
    }

    @Before
    public void checkConsistency() throws IOException {
	check.runCheck();
    }
}