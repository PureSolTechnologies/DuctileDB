package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AddVertexOperation extends AbstractTxOperation {

    private final long vertexId;
    private final Set<String> labels;
    private final Map<String, Object> properties;

    public AddVertexOperation(DuctileDBTransactionImpl transaction, long vertexId, Set<String> labels,
	    Map<String, Object> properties) {
	super(transaction);
	this.vertexId = vertexId;
	this.labels = Collections.unmodifiableSet(labels);
	this.properties = Collections.unmodifiableMap(properties);
	transaction.setCachedVertex(new DuctileDBVertexImpl(transaction.getGraph(), this.vertexId, this.labels,
		this.properties, new ArrayList<>()));
    }

    public long getVertexId() {
	return vertexId;
    }

    public Set<String> getLabels() {
	return labels;
    }

    public Map<String, Object> getProperties() {
	return properties;
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	List<Put> labelIndex = new ArrayList<>();
	for (String label : labels) {
	    put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), new byte[0]);
	    labelIndex.add(OperationsHelper.createVertexLabelIndexPut(vertexId, label));
	}
	List<Put> propertyIndex = new ArrayList<>();
	for (Entry<String, Object> property : properties.entrySet()) {
	    String key = property.getKey();
	    Object value = property.getValue();
	    put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		    SerializationUtils.serialize((Serializable) value));
	    propertyIndex.add(
		    OperationsHelper.createVertexPropertyIndexPut(vertexId, property.getKey(), property.getValue()));
	}
	put(VERTICES_TABLE, put);
	put(VERTEX_LABELS_INDEX_TABLE, labelIndex);
	put(VERTEX_PROPERTIES_INDEX_TABLE, propertyIndex);
    }
}