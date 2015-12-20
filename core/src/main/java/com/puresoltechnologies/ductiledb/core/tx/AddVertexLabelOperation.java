package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AddVertexLabelOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String label;
    private final boolean wasPresent;

    public AddVertexLabelOperation(DuctileDBTransactionImpl transaction, long vertexId, String label) {
	super(transaction);
	this.vertexId = vertexId;
	this.label = label;
	this.wasPresent = getTransaction().getVertex(vertexId).hasLabel(label);
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	DuctileDBVertexImpl vertex = (DuctileDBVertexImpl) transaction.getVertex(vertexId);
	vertex.addLabelInternally(label);
    }

    @Override
    public void rollbackInternally() {
	if (!wasPresent) {
	    DuctileDBTransactionImpl transaction = getTransaction();
	    DuctileDBVertexImpl vertex = (DuctileDBVertexImpl) transaction.getVertex(vertexId);
	    vertex.removeLabelInternally(label);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), Bytes.toBytes(label));
	Put index = OperationsHelper.createVertexLabelIndexPut(vertexId, label);
	put(SchemaTable.VERTICES.getTableName(), put);
	put(SchemaTable.VERTEX_LABELS.getTableName(), index);
    }
}
