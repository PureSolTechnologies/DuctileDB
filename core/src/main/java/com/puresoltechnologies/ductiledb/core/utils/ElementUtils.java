package com.puresoltechnologies.ductiledb.core.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBElement;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.DuctileDBException;

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
     * @return
     */
    public static List<DuctileDBEdge> getEdges(DuctileDBVertex vertex) {
	List<DuctileDBEdge> edges = new ArrayList<>();
	vertex.getEdges(EdgeDirection.BOTH).forEach(edge -> edges.add(edge));
	return edges;
    }

    /**
     * This method returns the labels of a vertex as {@link Set}.
     * 
     * @param vertex
     * @return
     */
    public static Set<String> getLabels(DuctileDBVertex vertex) {
	Set<String> labels = new HashSet<>();
	vertex.getLabels().forEach(label -> labels.add(label));
	return labels;
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
}