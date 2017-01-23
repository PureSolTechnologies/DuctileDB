package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.io.Serializable;

import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class SetVertexPropertyOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String key;
    private final Object value;
    private final Object oldValue;

    public SetVertexPropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String key,
	    Object value) {
	super(transaction);
	this.vertexId = vertex.getId();
	this.key = key;
	this.value = value;
	this.oldValue = vertex.getProperty(key);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	vertex.setProperty(key, value);
    }

    @Override
    public void rollbackInternally() {
	DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	if (oldValue == null) {
	    vertex.removeProperty(key);
	} else {
	    vertex.setProperty(key, oldValue);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(Key.of(id));
	put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(key),
		ColumnValue.of(Serializer.serializePropertyValue((Serializable) value)));
	Put index = OperationsHelper.createVertexPropertyIndexPut(vertexId, key, (Serializable) value);
	put(DatabaseTable.VERTICES.getName(), put);
	put(DatabaseTable.VERTEX_PROPERTIES.getName(), index);
    }
}
