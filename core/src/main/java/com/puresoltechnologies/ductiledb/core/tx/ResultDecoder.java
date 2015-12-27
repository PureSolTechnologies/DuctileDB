package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.START_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.TARGET_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.VERICES_COLUMN_FAMILY_BYTES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.exceptions.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

/**
 * This class is used to convert {@link Result}s into objects.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ResultDecoder {

    public static DuctileDBCacheVertex toVertex(DuctileDBGraphImpl graph, long vertexId, Result result) {
	if (result.isEmpty()) {
	    return null;
	}
	// Reading labels...
	Set<String> labels = new HashSet<>();
	NavigableMap<byte[], byte[]> labelsMap = result.getFamilyMap(LABELS_COLUMN_FAMILIY_BYTES);
	if (labelsMap != null) {
	    for (byte[] label : labelsMap.keySet()) {
		labels.add(Bytes.toString(label));
	    }
	}
	// Reading properties...
	Map<String, Object> properties = new HashMap<>();
	NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(PROPERTIES_COLUMN_FAMILY_BYTES);
	if (propertyMap != null) {
	    for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		String key = Bytes.toString(entry.getKey());
		if (!key.startsWith("~")) {
		    Object value = SerializationUtils.deserialize(entry.getValue());
		    properties.put(key, value);
		}
	    }
	}
	// Read edges...
	List<DuctileDBCacheEdge> edges = new ArrayList<>();
	NavigableMap<byte[], byte[]> edgesMap = result.getFamilyMap(EDGES_COLUMN_FAMILY_BYTES);
	if (edgesMap != null) {
	    for (Entry<byte[], byte[]> edge : edgesMap.entrySet()) {
		EdgeKey edgeKey = EdgeKey.decode(edge.getKey());
		EdgeValue edgeValue = EdgeValue.decode(edge.getValue());
		if (EdgeDirection.IN == edgeKey.getDirection()) {
		    edges.add(new DuctileDBCacheEdge(graph, edgeKey.getId(), edgeKey.getLabel(), edgeKey.getVertexId(),
			    vertexId, edgeValue.getProperties()));
		} else {
		    edges.add(new DuctileDBCacheEdge(graph, edgeKey.getId(), edgeKey.getLabel(), vertexId,
			    edgeKey.getVertexId(), edgeValue.getProperties()));
		}
	    }
	}
	return new DuctileDBCacheVertex(graph, vertexId, labels, properties, edges);
    }

    public static DuctileDBCacheEdge toCacheEdge(DuctileDBGraphImpl graph, long edgeId, Result result) {
	if (result.isEmpty()) {
	    return null;
	}
	NavigableMap<byte[], byte[]> verticesColumnFamily = result.getFamilyMap(VERICES_COLUMN_FAMILY_BYTES);
	long startVertexId = IdEncoder.decodeRowId(verticesColumnFamily.get(START_VERTEXID_COLUMN_BYTES));
	long targetVertexId = IdEncoder.decodeRowId(verticesColumnFamily.get(TARGET_VERTEXID_COLUMN_BYTES));
	NavigableMap<byte[], byte[]> labelsMap = result.getFamilyMap(LABELS_COLUMN_FAMILIY_BYTES);
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
	NavigableMap<byte[], byte[]> propertiesMap = result.getFamilyMap(PROPERTIES_COLUMN_FAMILY_BYTES);
	for (Entry<byte[], byte[]> property : propertiesMap.entrySet()) {
	    String key = Bytes.toString(property.getKey());
	    if (!key.startsWith("~")) {
		Object value = SerializationUtils.deserialize(property.getValue());
		properties.put(key, value);
	    }
	}
	return new DuctileDBCacheEdge(graph, edgeId, label, startVertexId, targetVertexId, properties);
    }
}
