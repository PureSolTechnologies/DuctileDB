package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

/**
 * A collection of simple methods to support testing.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTestHelper {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBTestHelper.class);

    /**
     * Runs through an {@link Iterable} and counts the number of elements.
     * 
     * @param iterable
     *            is the {@link Iterable} to count the elements in.
     * @return A long value with the number of elements is returned.
     */
    public static long count(Iterable<?> iterable) {
	long[] count = { 0 };
	iterable.forEach(c -> count[0]++);
	return count[0];
    }

    /**
     * Runs through an {@link Iterator} and counts the number of elements.
     * 
     * @param iterator
     *            is the {@link Iterator} to count the elements in.
     * @return The current number of elements is returned.
     */
    public static long count(Iterator<?> iterator) {
	long[] count = { 0 };
	iterator.forEachRemaining(c -> count[0]++);
	return count[0];
    }

    public static void removeGraph(GraphStore graph) throws IOException, StorageException {
	logger.info("Delete ductile graph...");
	for (DuctileDBEdge edge : graph.getEdges()) {
	    edge.remove();
	}
	for (DuctileDBVertex vertex : graph.getVertices()) {
	    vertex.remove();
	}
	graph.commit();
	DuctileDBGraphManager graphManager = graph.createGraphManager();
	for (String variableName : graphManager.getVariableNames()) {
	    graphManager.removeVariable(variableName);
	}
	DuctileDBSchemaManager schemaManager = graph.createSchemaManager();
	for (String typeName : schemaManager.getDefinedTypes()) {
	    for (ElementType elementType : ElementType.values()) {
		schemaManager.removeTypeDefinition(elementType, typeName);
	    }
	}
	for (String propertyName : schemaManager.getDefinedProperties()) {
	    for (ElementType elementType : ElementType.values()) {
		schemaManager.removePropertyDefinition(elementType, propertyName);
	    }
	}
	assertEquals(GraphStoreImpl.class, graph.getClass());
	new DuctileDBHealthCheck((GraphStoreImpl) graph).runCheck();
	logger.info("Ductile graph deleted.");
    }
}
