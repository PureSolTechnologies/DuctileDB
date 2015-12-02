package com.puresoltechnologies.ductiledb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

/**
 * This class is used to convert {@link Result}s into objects.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ResultDecoder {

    public static DuctileDBVertexImpl toVertex(DuctileDBGraphImpl graph, long vertexId, Result result) {
	if (result.isEmpty()) {
	    return null;
	}
	// Reading labels...
	Set<String> labels = new HashSet<>();
	NavigableMap<byte[], byte[]> labelsMap = result.getFamilyMap(DuctileDBGraphImpl.LABELS_COLUMN_FAMILIY_BYTES);
	if (labelsMap != null) {
	    for (byte[] label : labelsMap.keySet()) {
		labels.add(Bytes.toString(label));
	    }
	}
	// Reading properties...
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<byte[], byte[]> propertyMap = result
		.getFamilyMap(DuctileDBGraphImpl.PROPERTIES_COLUMN_FAMILY_BYTES);
	if (propertyMap != null) {
	    for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		String key = Bytes.toString(entry.getKey());
		Object value = SerializationUtils.deserialize(entry.getValue());
		properties.put(key, value);
	    }
	}
	// Read edges...
	DuctileDBVertexImpl vertex = new DuctileDBVertexImpl(graph, vertexId, labels, properties, new ArrayList<>());
	NavigableMap<byte[], byte[]> edgesMap = result.getFamilyMap(DuctileDBGraphImpl.EDGES_COLUMN_FAMILY_BYTES);
	if (edgesMap != null) {
	    for (Entry<byte[], byte[]> edge : edgesMap.entrySet()) {
		EdgeKey edgeKey = EdgeKey.decode(edge.getKey());
		EdgeValue edgeValue = EdgeValue.decode(edge.getValue());
		if (EdgeDirection.IN == edgeKey.getDirection()) {
		    vertex.addEdge(new DuctileDBEdgeImpl(graph, edgeKey.getId(), edgeKey.getEdgeType(),
			    edgeKey.getVertexId(), vertex, edgeValue.getProperties()));
		} else {
		    vertex.addEdge(new DuctileDBEdgeImpl(graph, edgeKey.getId(), edgeKey.getEdgeType(), vertex,
			    edgeKey.getVertexId(), edgeValue.getProperties()));
		}
	    }
	}
	return vertex;
    }

    public static DuctileDBEdgeImpl toEdge(DuctileDBGraphImpl graph, long edgeId, Result result) {
	if (result.isEmpty()) {
	    return null;
	}
	NavigableMap<byte[], byte[]> verticesColumnFamily = result
		.getFamilyMap(DuctileDBGraphImpl.VERICES_COLUMN_FAMILY_BYTES);
	long startVertexId = IdEncoder
		.decodeRowId(verticesColumnFamily.get(DuctileDBGraphImpl.START_VERTEXID_COLUMN_BYTES));
	long targetVertexId = IdEncoder
		.decodeRowId(verticesColumnFamily.get(DuctileDBGraphImpl.TARGET_VERTEXID_COLUMN_BYTES));
	NavigableMap<byte[], byte[]> labelsMap = result.getFamilyMap(DuctileDBGraphImpl.LABELS_COLUMN_FAMILIY_BYTES);
	Set<byte[]> labelBytes = labelsMap.keySet();
	if (labelBytes.size() == 0) {
	    throw new DuctileDBException("Found edge without label (id='" + edgeId
		    + "'). This is not supported and an inconsistency in graph.");
	}
	if (labelBytes.size() > 1) {
	    throw new DuctileDBException("Found edge with multiple labels (id='" + edgeId
		    + "'). This is not supported and an inconsistency in graph.");
	}
	String label = Bytes.toString(labelBytes.iterator().next());
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<byte[], byte[]> propertiesMap = result
		.getFamilyMap(DuctileDBGraphImpl.PROPERTIES_COLUMN_FAMILY_BYTES);
	for (Entry<byte[], byte[]> property : propertiesMap.entrySet()) {
	    String key = Bytes.toString(property.getKey());
	    Object value = SerializationUtils.deserialize(property.getValue());
	    properties.put(key, value);
	}
	return new DuctileDBEdgeImpl(graph, edgeId, label, startVertexId, targetVertexId, properties);
    }
}
