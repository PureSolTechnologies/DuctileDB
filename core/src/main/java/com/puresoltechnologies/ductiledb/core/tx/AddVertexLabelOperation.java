package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AddVertexLabelOperation extends AbstractTxOperation {

    private final DuctileDBVertex vertex;
    private final String label;

    public AddVertexLabelOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String label) {
	super(transaction);
	this.vertex = vertex;
	this.label = label;
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	Set<String> labels = ElementUtils.getLabels(vertex);
	labels.add(label);
	DuctileDBVertexImpl cachedVertex = new DuctileDBVertexImpl(transaction.getGraph(), vertex.getId(), labels,
		ElementUtils.getProperties(vertex), ElementUtils.getEdges(vertex));
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
	Put put = new Put(id);
	put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), Bytes.toBytes(label));
	Put index = OperationsHelper.createVertexLabelIndexPut(vertexId, label);
	put(SchemaTable.VERTICES.getTableName(), put);
	put(SchemaTable.VERTEX_LABELS.getTableName(), index);
    }
}
