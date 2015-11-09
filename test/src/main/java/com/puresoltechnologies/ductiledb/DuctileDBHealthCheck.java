package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.utils.IdEncoder;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * This class is used to check for consistency in DuctileDB. It is primarily
 * used for testing to check consistency for indices, duplicated storage and so
 * forth.
 * 
 * <b>Attention:</b> The check is quite expensive and includes a duplicated read
 * of the whole database. It is for small databases during testing, but not for
 * productive use.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBHealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBHealthCheck.class);

    private final Connection connection;
    private final DuctileDBGraphImpl graph;

    public DuctileDBHealthCheck(Connection connection) throws IOException {
	super();
	this.connection = connection;
	graph = (DuctileDBGraphImpl) GraphFactory.createGraph(connection);
    }

    public void runCheck() throws IOException {
	checkForCorrectVertexIndices();
	checkForCorrectEdgeIndices();
	checkVertexLabelIndex();
	checkVertexPropertyIndex();
	checkEdgeLabelIndex();
	checkEdgePropertyIndex();
    }

    /**
     * This method reads all vertices and checks the correct presence of all
     * indices.
     * 
     * @throws IOException
     */
    private void checkForCorrectVertexIndices() throws IOException {
	logger.info("Check vertices...");
	Iterable<Vertex> vertices = graph.getVertices();
	for (Vertex tempVertex : vertices) {
	    DuctileDBVertex vertex = (DuctileDBVertex) tempVertex;
	    logger.info("Checking '" + vertex + "'...");
	    assertEquals(vertex, graph.getVertex(vertex.getId()));
	    for (String label : vertex.getLabels()) {
		try (Table table = graph.openVertexLabelTable()) {
		    Result result = table.get(new Get(Bytes.toBytes(label)));
		    assertFalse("Could not find row for label '" + label + "' in vertex label index.",
			    result.isEmpty());
		    Cell cell = result.getColumnLatestCell(DuctileDBGraphImpl.INDEX_COLUMN_FAMILY_BYTES,
			    IdEncoder.encodeRowId(vertex.getId()));
		    assertNotNull("Could not find vertex label index entry for label '" + label
			    + "' for vertex with id '" + vertex.getId() + "'", cell);
		}
	    }
	}
	// TODO
	logger.info("Vertices checked.");
    }

    /**
     * This method reads all edges and checks the correct presence of all
     * indices.
     * 
     * @throws IOException
     */
    private void checkForCorrectEdgeIndices() throws IOException {
	logger.info("Check edges...");
	Iterable<Edge> edges = graph.getEdges();
	for (Edge tempEdge : edges) {
	    DuctileDBEdge edge = (DuctileDBEdge) tempEdge;
	    assertEquals(edge, graph.getVertex(edge.getId()));
	    String label = edge.getLabel();
	    try (Table table = graph.openVertexLabelTable()) {
		Result result = table.get(new Get(Bytes.toBytes(label)));
		assertFalse("Could not find row for label '" + label + "' in edge label index.", result.isEmpty());
		Cell cell = result.getColumnLatestCell(DuctileDBGraphImpl.INDEX_COLUMN_FAMILY_BYTES,
			IdEncoder.encodeRowId(edge.getId()));
		assertNotNull("Could not find edge label index entry for label '" + label + "' for edge with id '"
			+ edge.getId() + "'", cell);
	    }
	}
	// TODO
	logger.info("Edges checked.");
    }

    /**
     * This method reads the complete vertex label index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkVertexLabelIndex() {
	// TODO Auto-generated method stub

    }

    /**
     * This method reads the complete vertex property index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkVertexPropertyIndex() {
	// TODO Auto-generated method stub

    }

    /**
     * This method reads the complete edge label index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkEdgeLabelIndex() {
	// TODO Auto-generated method stub

    }

    /**
     * This method reads the complete edge property index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkEdgePropertyIndex() {
	// TODO Auto-generated method stub
    }

}
