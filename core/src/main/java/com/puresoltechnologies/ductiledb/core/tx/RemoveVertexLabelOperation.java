package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexLabelOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String label;

    public RemoveVertexLabelOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String label) {
	super(transaction);
	this.vertexId = vertex.getId();
	this.label = label;
	Set<String> labels = ElementUtils.getLabels(vertex);
	labels.remove(label);
	DuctileDBVertexImpl cachedVertex = new DuctileDBVertexImpl(transaction.getGraph(), vertex.getId(), labels,
		ElementUtils.getProperties(vertex), ElementUtils.getEdges(vertex));
	transaction.setCachedVertex(cachedVertex);
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
