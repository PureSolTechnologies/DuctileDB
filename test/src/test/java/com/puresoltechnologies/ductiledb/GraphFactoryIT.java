package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.AbstractHGraphTest;
import com.puresoltechnologies.ductiledb.GraphFactory;
import com.puresoltechnologies.ductiledb.HGraph;

public class GraphFactoryIT extends AbstractHGraphTest {

    @Test
    public void testConnection() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {

	}
    }

}
