package com.puresoltechnologies.ductiledb.core.tx.ops;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexLabelOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String label;

    public RemoveVertexLabelOperation(Connection connection, long vertexId, String label) {
	super(connection);
	this.vertexId = vertexId;
	this.label = label;
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label));
	Delete index = OperationsHelper.createVertexLabelIndexDelete(vertexId, label);
	delete(VERTICES_TABLE, delete);
	delete(VERTEX_LABELS_INDEX_TABLE, index);
    }

}