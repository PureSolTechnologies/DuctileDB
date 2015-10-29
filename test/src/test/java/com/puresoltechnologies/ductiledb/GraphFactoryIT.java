package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.GraphFactory;
import com.puresoltechnologies.ductiledb.DuctileDBGraph;

public class GraphFactoryIT extends AbstractDuctileDBTest {

    @Test
    public void testConnection() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {

	}
    }

}
