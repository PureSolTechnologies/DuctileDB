package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.HBaseSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.HBaseSchema.DUCTILEDB_ID_PROPERTY;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.utils.Serializer;

public class AddVertexOperation extends AbstractTxOperation {

    private final Set<String> types;
    private final Map<String, Object> properties;
    private final DuctileDBCacheVertex vertex;

    public AddVertexOperation(DuctileDBTransactionImpl transaction, long vertexId, Set<String> types,
	    Map<String, Object> properties) {
	super(transaction);
	this.types = Collections.unmodifiableSet(types);
	this.properties = Collections.unmodifiableMap(properties);
	this.vertex = new DuctileDBCacheVertex(transaction, vertexId, types, properties, new ArrayList<>());
    }

    public long getVertexId() {
	return vertex.getId();
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.setCachedVertex(vertex);
    }

    @Override
    public void rollbackInternally() {
	// Intentionally left empty...
    }

    @Override
    public void perform() throws IOException {
	long vertexId = vertex.getId();
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	List<Put> typeIndex = new ArrayList<>();
	for (String type : types) {
	    put.addColumn(HBaseColumnFamily.TYPES.getNameBytes(), Bytes.toBytes(type), new byte[0]);
	    typeIndex.add(OperationsHelper.createVertexTypeIndexPut(vertexId, type));
	}
	List<Put> propertyIndex = new ArrayList<>();
	put.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(DUCTILEDB_ID_PROPERTY),
		Serializer.serializePropertyValue(vertexId));
	put.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY),
		Serializer.serializePropertyValue(new Date()));
	for (Entry<String, Object> property : properties.entrySet()) {
	    String key = property.getKey();
	    Object value = property.getValue();
	    put.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(key),
		    Serializer.serializePropertyValue((Serializable) value));
	    propertyIndex.add(OperationsHelper.createVertexPropertyIndexPut(vertexId, property.getKey(),
		    (Serializable) property.getValue()));
	}
	put(HBaseTable.VERTICES.getTableName(), put);
	put(HBaseTable.VERTEX_TYPES.getTableName(), typeIndex);
	put(HBaseTable.VERTEX_PROPERTIES.getTableName(), propertyIndex);
    }
}