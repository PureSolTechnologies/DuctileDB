package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

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

    private final Connection connection;
    private final DuctileDBGraphImpl graph;

    public DuctileDBHealthCheck(Connection connection) throws IOException {
	super();
	this.connection = connection;
	graph = (DuctileDBGraphImpl) GraphFactory.createGraph(connection);
    }

    public void runCheck() {
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
     */
    private void checkForCorrectVertexIndices() {
	// TODO Auto-generated method stub
	graph.getVertices();
    }

    /**
     * This method reads all edges and checks the correct presence of all
     * indices.
     */
    private void checkForCorrectEdgeIndices() {
	// TODO Auto-generated method stub

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
