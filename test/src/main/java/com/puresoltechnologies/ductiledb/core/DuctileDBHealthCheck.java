package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.Direction;
import com.puresoltechnologies.ductiledb.api.Edge;
import com.puresoltechnologies.ductiledb.api.Vertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

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

    private final DuctileDBGraphImpl graph;

    public DuctileDBHealthCheck(DuctileDBGraphImpl graph) throws IOException {
	super();
	this.graph = graph;
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
	for (Vertex vertex : vertices) {
	    logger.info("Checking '" + vertex + "'...");
	    assertEquals(vertex, graph.getVertex(vertex.getId()));
	    try (Table table = graph.openVertexLabelTable()) {
		for (String label : vertex.getLabels()) {
		    Result result = table.get(new Get(Bytes.toBytes(label)));
		    assertFalse("Could not find row for label '" + label + "' in vertex label index.",
			    result.isEmpty());
		    byte[] value = result.getFamilyMap(DuctileDBGraphImpl.INDEX_COLUMN_FAMILY_BYTES)
			    .get(IdEncoder.encodeRowId(vertex.getId()));
		    assertNotNull("Could not find vertex label index entry for label '" + label
			    + "' for vertex with id '" + vertex.getId() + "'", value);
		}
	    }
	    try (Table table = graph.openVertexPropertyTable()) {
		for (String key : vertex.getPropertyKeys()) {
		    Object value = vertex.getProperty(key);
		    Result result = table.get(new Get(Bytes.toBytes(key)));
		    assertFalse("Could not find row for property '" + key + "' in vertex property index.",
			    result.isEmpty());
		    NavigableMap<byte[], byte[]> familyMap = result
			    .getFamilyMap(DuctileDBGraphImpl.INDEX_COLUMN_FAMILY_BYTES);
		    assertNotNull("Could not find vertex property index entry for property '" + key
			    + "' for vertex with id '" + vertex.getId() + "'", familyMap);
		    Object deserialized = SerializationUtils
			    .deserialize(familyMap.get(IdEncoder.encodeRowId(vertex.getId())));
		    assertEquals("Value for vertex property '" + key + "' for vertex '" + vertex.getId()
			    + "' is not as expected. ", value, deserialized);
		}
	    }
	    for (Edge edge : vertex.getEdges(Direction.BOTH)) {
		Edge readEdge = graph.getEdge(edge.getId());
		assertEquals("Separately stored edge is not equals.", edge, readEdge);
	    }
	}
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
	for (Edge edge : edges) {
	    assertEquals(edge, graph.getEdge(edge.getId()));
	    String label = edge.getLabel();
	    try (Table table = graph.openEdgeLabelTable()) {
		Result result = table.get(new Get(Bytes.toBytes(label)));
		assertFalse("Could not find row for label '" + label + "' in edge label index.", result.isEmpty());
		byte[] value = result.getFamilyMap(DuctileDBGraphImpl.INDEX_COLUMN_FAMILY_BYTES)
			.get(IdEncoder.encodeRowId(edge.getId()));
		assertNotNull("Could not find edge label index entry for label '" + label + "' for edge with id '"
			+ edge.getId() + "'", value);
	    }
	    try (Table table = graph.openEdgePropertyTable()) {
		for (String key : edge.getPropertyKeys()) {
		    Object value = edge.getProperty(key);
		    Result result = table.get(new Get(Bytes.toBytes(key)));
		    assertFalse("Could not find row for property '" + key + "' in edge property index.",
			    result.isEmpty());
		    NavigableMap<byte[], byte[]> familyMap = result
			    .getFamilyMap(DuctileDBGraphImpl.INDEX_COLUMN_FAMILY_BYTES);
		    assertNotNull("Could not find edge property index entry for property '" + key
			    + "' for edge with id '" + edge.getId() + "'", familyMap);
		    Object deserialized = SerializationUtils
			    .deserialize(familyMap.get(IdEncoder.encodeRowId(edge.getId())));
		    assertEquals("Value for edge property '" + key + "' for edge '" + edge.getId()
			    + "' is not as expected. ", value, deserialized);
		}
	    }
	    Vertex startVertex = edge.getStartVertex();
	    assertNotNull("Start vertex is not present.", startVertex);
	    Vertex targetVertex = edge.getTargetVertex();
	    assertNotNull("Target vertex is not present.", targetVertex);
	}
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
