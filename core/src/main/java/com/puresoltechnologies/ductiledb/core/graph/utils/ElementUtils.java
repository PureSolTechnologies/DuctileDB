package com.puresoltechnologies.ductiledb.core.graph.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBException;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBElement;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBAttachedEdge;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBAttachedVertex;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBDetachedEdge;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBDetachedVertex;
import com.puresoltechnologies.ductiledb.core.graph.tx.DuctileDBTransactionImpl;

/**
 * This class contains helper methods to handle Ductile DB elements.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ElementUtils {

    /**
     * Private constructor to avoid instantiation.
     */
    private ElementUtils() {
    }

    /**
     * This method creates a {@link Map} of properties.
     * 
     * @param element
     *            is the {@link DuctileDBElement} for which the properties are
     *            to be returned.
     * @return A {@link Map} is returned containing the properties.
     */
    public static Map<String, Object> getProperties(DuctileDBElement element) {
	Map<String, Object> properties = new HashMap<>();
	element.getPropertyKeys().forEach(key -> properties.put(key, element.getProperty(key)));
	return properties;
    }

    /**
     * This method returns a {@link List} of {@link DuctileDBEdge}.
     * 
     * @param vertex
     *            is the vertex for which the edges are to be returned.
     * @return A {@link List} of {@link DuctileDBEdge} is returned.
     */
    public static List<DuctileDBEdge> getEdges(DuctileDBVertex vertex) {
	List<DuctileDBEdge> edges = new ArrayList<>();
	vertex.getEdges(EdgeDirection.BOTH).forEach(edge -> edges.add(edge));
	return edges;
    }

    /**
     * This method returns the types of a vertex as {@link Set}.
     * 
     * @param vertex
     *            is the vertex for which the types are to be returned.
     * @return A {@link Set} of type names is returned.
     */
    public static Set<String> getTypes(DuctileDBVertex vertex) {
	Set<String> types = new HashSet<>();
	vertex.getTypes().forEach(type -> types.add(type));
	return types;
    }

    public static <T> void setFinalField(Object object, Class<?> clazz, String fieldName, T value) {
	try {
	    Field field = clazz.getDeclaredField(fieldName);
	    field.setAccessible(true);
	    field.set(object, value);
	} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
	    throw new DuctileDBException("Could not set field '" + fieldName + "' in class '" + object.getClass()
		    + "' to value '" + value + "'.", e);
	}
    }

    public static DuctileDBAttachedVertex toAttached(DuctileDBVertex vertex) {
	return new DuctileDBAttachedVertex((DuctileDBTransactionImpl) vertex.getTransaction(), vertex.getId());
    }

    public static DuctileDBDetachedVertex toDetached(DuctileDBVertex vertex) {
	return new DuctileDBDetachedVertex((DuctileDBTransactionImpl) vertex.getTransaction(), vertex.getId(),
		ElementUtils.getTypes(vertex), ElementUtils.getProperties(vertex), ElementUtils.getEdges(vertex));
    }

    public static DuctileDBAttachedEdge toAttached(DuctileDBEdge edge) {
	return new DuctileDBAttachedEdge((DuctileDBTransactionImpl) edge.getTransaction(), edge.getId());
    }

    public static DuctileDBDetachedEdge toDetached(DuctileDBEdge edge) {
	return new DuctileDBDetachedEdge((DuctileDBTransactionImpl) edge.getTransaction(), edge.getId(), edge.getType(),
		edge.getStartVertex(), edge.getTargetVertex(), ElementUtils.getProperties(edge));
    }
}
