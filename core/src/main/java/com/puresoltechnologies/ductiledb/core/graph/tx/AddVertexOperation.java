package com.puresoltechnologies.ductiledb.core.graph.tx;

import static com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema.DUCTILEDB_ID_PROPERTY;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.logstore.Key;

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
	Put put = new Put(Key.of(id));
	List<Put> typeIndex = new ArrayList<>();
	for (String type : types) {
	    put.addColumn(DatabaseColumnFamily.TYPES.getKey(), Key.of(type), ColumnValue.empty());
	    typeIndex.add(OperationsHelper.createVertexTypeIndexPut(vertexId, type));
	}
	List<Put> propertyIndex = new ArrayList<>();
	put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(DUCTILEDB_ID_PROPERTY),
		ColumnValue.of(Serializer.serializePropertyValue(vertexId)));
	put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY),
		ColumnValue.of(Serializer.serializePropertyValue(new Date())));
	for (Entry<String, Object> property : properties.entrySet()) {
	    String key = property.getKey();
	    Object value = property.getValue();
	    put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(key),
		    ColumnValue.of(Serializer.serializePropertyValue((Serializable) value)));
	    propertyIndex.add(OperationsHelper.createVertexPropertyIndexPut(vertexId, property.getKey(),
		    (Serializable) property.getValue()));
	}
	put(DatabaseTable.VERTICES.getName(), put);
	put(DatabaseTable.VERTEX_TYPES.getName(), typeIndex);
	put(DatabaseTable.VERTEX_PROPERTIES.getName(), propertyIndex);
    }
}