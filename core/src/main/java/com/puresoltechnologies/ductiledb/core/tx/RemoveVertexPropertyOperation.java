package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexPropertyOperation extends AbstractTxOperation {

    private final DuctileDBVertex vertex;
    private final String key;

    public RemoveVertexPropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String key) {
	super(transaction);
	this.vertex = vertex;
	this.key = key;
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	Map<String, Object> properties = ElementUtils.getProperties(vertex);
	properties.remove(key);
	DuctileDBVertexImpl cachedVertex = new DuctileDBVertexImpl(transaction.getGraph(), vertex.getId(),
		ElementUtils.getLabels(vertex), properties, ElementUtils.getEdges(vertex));
	transaction.setCachedVertex(cachedVertex);

    }

    @Override
    public void rollbackInternally() {
	// TODO Auto-generated method stub
    }

    @Override
    public void perform() throws IOException {
	long vertexId = vertex.getId();
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	Delete index = OperationsHelper.createVertexPropertyIndexDelete(vertexId, key);
	delete(SchemaTable.VERTICES.getTableName(), delete);
	delete(SchemaTable.VERTEX_PROPERTIES.getTableName(), index);
    }
}
