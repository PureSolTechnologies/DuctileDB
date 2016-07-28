package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBException;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

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
	NavigableMap<byte[], byte[]> typesMap = result.getFamilyMap(DatabaseColumnFamily.TYPES.getNameBytes());
	if (typesMap != null) {
	    for (byte[] type : typesMap.keySet()) {
		types.add(Bytes.toString(type));
	    }
	}
	// Reading properties...
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(DatabaseColumnFamily.PROPERTIES.getNameBytes());
	if (propertyMap != null) {
	    for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		String key = Bytes.toString(entry.getKey());
		if (!key.startsWith("~")) {
		    Object value = Serializer.deserializePropertyValue(entry.getValue());
		    properties.put(key, value);
		}
	    }
	}
	// Read edges...
	List<DuctileDBCacheEdge> edges = new ArrayList<>();
	NavigableMap<byte[], byte[]> edgesMap = result.getFamilyMap(DatabaseColumnFamily.EDGES.getNameBytes());
	if (edgesMap != null) {
	    for (Entry<byte[], byte[]> edge : edgesMap.entrySet()) {
		EdgeKey edgeKey = EdgeKey.decode(edge.getKey());
		EdgeValue edgeValue = EdgeValue.decode(edge.getValue());
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
	NavigableMap<byte[], byte[]> verticesColumnFamily = result
		.getFamilyMap(DatabaseColumnFamily.VERICES.getNameBytes());
	long startVertexId = IdEncoder
		.decodeRowId(verticesColumnFamily.get(DatabaseColumn.START_VERTEX_ID.getNameBytes()));
	long targetVertexId = IdEncoder
		.decodeRowId(verticesColumnFamily.get(DatabaseColumn.TARGET_VERTEX_ID.getNameBytes()));
	NavigableMap<byte[], byte[]> typesMap = result.getFamilyMap(DatabaseColumnFamily.TYPES.getNameBytes());
	Set<byte[]> typeBytes = typesMap.keySet();
	if (typeBytes.size() == 0) {
	    throw new DuctileDBException("Found edge without type (id='" + edgeId
		    + "'). This is not supported and an inconsistency in graph.");
	}
	if (typeBytes.size() > 1) {
	    throw new DuctileDBException("Found edge with multiple types (id='" + edgeId
		    + "'). This is not supported and an inconsistency in graph.");
	}
	String type = Bytes.toString(typeBytes.iterator().next());
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<byte[], byte[]> propertiesMap = result.getFamilyMap(DatabaseColumnFamily.PROPERTIES.getNameBytes());
	for (Entry<byte[], byte[]> property : propertiesMap.entrySet()) {
	    String key = Bytes.toString(property.getKey());
	    if (!key.startsWith("~")) {
		Object value = Serializer.deserializePropertyValue(property.getValue());
		properties.put(key, value);
	    }
	}
	return new DuctileDBCacheEdge(transaction, edgeId, type, startVertexId, targetVertexId, properties);
    }
}
