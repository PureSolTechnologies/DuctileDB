package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.Result;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.core.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.logstore.Key;

/**
 * This class is used to convert {@link Result}s into objects.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ResultDecoder {

    public static DuctileDBCacheVertex toVertex(DuctileDBTransactionImpl transaction, long vertexId, Result result) {
	if (result.isEmpty()) {
	    return null;
	}
	// Reading types...
	Set<String> types = new HashSet<>();
	NavigableMap<Key, ColumnValue> typesMap = result.getFamilyMap(DatabaseColumnFamily.TYPES.getKey());
	if (typesMap != null) {
	    for (Key type : typesMap.keySet()) {
		types.add(type.toString());
	    }
	}
	// Reading properties...
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<Key, ColumnValue> propertyMap = result.getFamilyMap(DatabaseColumnFamily.PROPERTIES.getKey());
	if (propertyMap != null) {
	    for (Entry<Key, ColumnValue> entry : propertyMap.entrySet()) {
		String key = entry.getKey().toString();
		if (!key.startsWith("~")) {
		    Object value = Serializer.deserializePropertyValue(entry.getValue().getBytes());
		    properties.put(key, value);
		}
	    }
	}
	// Read edges...
	List<DuctileDBCacheEdge> edges = new ArrayList<>();
	NavigableMap<Key, ColumnValue> edgesMap = result.getFamilyMap(DatabaseColumnFamily.EDGES.getKey());
	if (edgesMap != null) {
	    for (Entry<Key, ColumnValue> edge : edgesMap.entrySet()) {
		EdgeKey edgeKey = EdgeKey.decode(edge.getKey().getBytes());
		EdgeValue edgeValue = EdgeValue.decode(edge.getValue().getBytes());
		if (EdgeDirection.IN == edgeKey.getDirection()) {
		    edges.add(new DuctileDBCacheEdge(transaction, edgeKey.getId(), edgeKey.getType(),
			    edgeKey.getVertexId(), vertexId, edgeValue.getProperties()));
		} else {
		    edges.add(new DuctileDBCacheEdge(transaction, edgeKey.getId(), edgeKey.getType(), vertexId,
			    edgeKey.getVertexId(), edgeValue.getProperties()));
		}
	    }
	}
	return new DuctileDBCacheVertex(transaction, vertexId, types, properties, edges);
    }

    public static DuctileDBCacheEdge toCacheEdge(DuctileDBTransactionImpl transaction, long edgeId, Result result) {
	if (result.isEmpty()) {
	    return null;
	}
	NavigableMap<Key, ColumnValue> verticesColumnFamily = result
		.getFamilyMap(DatabaseColumnFamily.VERICES.getKey());
	long startVertexId = IdEncoder
		.decodeRowId(verticesColumnFamily.get(DatabaseColumn.START_VERTEX_ID.getKey()).getBytes());
	long targetVertexId = IdEncoder
		.decodeRowId(verticesColumnFamily.get(DatabaseColumn.TARGET_VERTEX_ID.getKey()).getBytes());
	NavigableMap<Key, ColumnValue> typesMap = result.getFamilyMap(DatabaseColumnFamily.TYPES.getKey());
	Set<Key> typeBytes = typesMap.keySet();
	if (typeBytes.size() == 0) {
	    throw new DuctileDBException("Found edge without type (id='" + edgeId
		    + "'). This is not supported and an inconsistency in graph.");
	}
	if (typeBytes.size() > 1) {
	    throw new DuctileDBException("Found edge with multiple types (id='" + edgeId
		    + "'). This is not supported and an inconsistency in graph.");
	}
	String type = typeBytes.iterator().next().toString();
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<Key, ColumnValue> propertiesMap = result.getFamilyMap(DatabaseColumnFamily.PROPERTIES.getKey());
	for (Entry<Key, ColumnValue> property : propertiesMap.entrySet()) {
	    String key = property.getKey().toString();
	    if (!key.startsWith("~")) {
		Object value = Serializer.deserializePropertyValue(property.getValue().getBytes());
		properties.put(key, value);
	    }
	}
	return new DuctileDBCacheEdge(transaction, edgeId, type, startVertexId, targetVertexId, properties);
    }
}
