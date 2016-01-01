package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;

/**
 * This class contains helpers to create some standard graphs for testing.
 * 
 * @author Rick-Rainer Ludwig
 */
public class StandardGraphs {

    /**
     * <p>
     * This method creates the specified number of vertices and connect each
     * vertex with all other vertices which have a higher id.
     * </p>
     * <p>
     * <b>Beware: The commit on the graph needs to be performed by the test!</b>
     * </p>
     * 
     * @param graph
     * @param numberOfVertices
     * @throws IOException
     */
    public static void createGraph(DuctileDBGraph graph, int numberOfVertices) throws IOException {
	DuctileDBVertex[] vertices = new DuctileDBVertex[numberOfVertices];
	for (int i = 0; i < numberOfVertices; ++i) {
	    Set<String> types = new HashSet<>();
	    types.add("vertex" + i);
	    DuctileDBVertex vertex = graph.addVertex(types, new HashMap<>());
	    vertices[i] = vertex;
	}
	for (int i = 0; i < numberOfVertices - 1; ++i) {
	    for (int j = i + 1; j < numberOfVertices; ++j) {
		graph.addEdge(vertices[i], vertices[j], "connects");
	    }
	}
    }

}
