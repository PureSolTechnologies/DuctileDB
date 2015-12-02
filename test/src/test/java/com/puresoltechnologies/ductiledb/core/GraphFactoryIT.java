package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;

public class GraphFactoryIT extends AbstractDuctileDBGraphTest {

    @Test
    public void testConnection() throws IOException {
	assertNotNull(graph);
	assertEquals(DuctileDBGraphImpl.class, graph.getClass());
    }

}
