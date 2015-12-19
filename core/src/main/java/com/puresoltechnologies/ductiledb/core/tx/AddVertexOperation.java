package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_ID_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
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
    }

    public long getVertexId() {
	return vertexId;
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.setCachedVertex(new DuctileDBVertexImpl(transaction.getGraph(), this.vertexId, this.labels,
		this.properties, new ArrayList<>()));

    }

    @Override
    public void rollbackInternally() {
	// TODO Auto-generated method stub
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
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(DUCTILEDB_ID_PROPERTY),
		SerializationUtils.serialize(vertexId));
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY),
		SerializationUtils.serialize(new Date()));
	for (Entry<String, Object> property : properties.entrySet()) {
	    String key = property.getKey();
	    Object value = property.getValue();
	    put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		    SerializationUtils.serialize((Serializable) value));
	    propertyIndex.add(
		    OperationsHelper.createVertexPropertyIndexPut(vertexId, property.getKey(), property.getValue()));
	}
	put(SchemaTable.VERTICES.getTableName(), put);
	put(SchemaTable.VERTEX_LABELS.getTableName(), labelIndex);
	put(SchemaTable.VERTEX_PROPERTIES.getTableName(), propertyIndex);
    }
}