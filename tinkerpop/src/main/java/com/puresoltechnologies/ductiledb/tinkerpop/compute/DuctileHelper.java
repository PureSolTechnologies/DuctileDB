package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertexProperty;

public class DuctileHelper {

    /**
     * Private constructor to avoid instantiation.
     */
    private DuctileHelper() {
    }

    public static Map<String, List<VertexProperty<?>>> getProperties(DuctileVertex vertex) {
	Map<String, List<VertexProperty<?>>> properties = new HashMap<>();
	Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
	if (properties != null) {
	    vertexProperties.forEachRemaining(p -> {
		String key = p.key();
		List<VertexProperty<?>> valueList = properties.get(key);
		if (valueList == null) {
		    valueList = new ArrayList<>();
		    properties.put(key, valueList);
		}
		valueList.add(new DuctileVertexProperty<>(vertex, key, p.value()));
	    });
	}
	return properties;
    }

}
