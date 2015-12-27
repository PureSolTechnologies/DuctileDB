package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexLabelOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String label;
    private final boolean wasPresent;

    public RemoveVertexLabelOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String label) {
	super(transaction);
	this.vertexId = vertex.getId();
	this.label = label;
	this.wasPresent = vertex.hasLabel(label);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	vertex.removeLabel(label);
    }

    @Override
    public void rollbackInternally() {
	if (!wasPresent) {
	    DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	    vertex.addLabel(label);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label));
	Delete index = OperationsHelper.createVertexLabelIndexDelete(vertexId, label);
	delete(SchemaTable.VERTICES.getTableName(), delete);
	delete(SchemaTable.VERTEX_LABELS.getTableName(), index);
    }
}
