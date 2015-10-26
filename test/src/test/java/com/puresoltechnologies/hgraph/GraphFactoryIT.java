package com.puresoltechnologies.hgraph;

import java.io.IOException;

import org.junit.Test;

public class GraphFactoryIT extends AbstractHGraphTest {

    @Test
    public void testConnection() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {

	}
    }

}
