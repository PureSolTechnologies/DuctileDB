package com.puresoltechnologies.ductiledb.core.tx.ops;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.INDEX_COLUMN_FAMILY_BYTES;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class OperationsHelper {

    /**
     * Private constructor to avoid instantiation.
     */
    private OperationsHelper() {
    }

    public static Put createVertexLabelIndexPut(long vertexId, String label) {
	Put labelIndexPut = new Put(Bytes.toBytes(label));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId), new byte[0]);
	return labelIndexPut;
    }

    public static Delete createVertexLabelIndexDelete(long vertexId, String label) {
	Delete labelIndexPut = new Delete(Bytes.toBytes(label));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId));
	return labelIndexPut;
    }

    public static Put createVertexPropertyIndexPut(long vertexId, String key, Object value) {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId),
		SerializationUtils.serialize((Serializable) value));
	return indexPut;
    }

    public static Delete createVertexPropertyIndexDelete(long vertexId, String key) {
	Delete indexPut = new Delete(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId));
	return indexPut;
    }

    public static Put createEdgeLabelIndexPut(long edgeId, String label) {
	Put labelIndexPut = new Put(Bytes.toBytes(label));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId), new byte[0]);
	return labelIndexPut;
    }

    public static Put createEdgePropertyIndexPut(long edgeId, String key, Object value) {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId),
		SerializationUtils.serialize((Serializable) value));
	return indexPut;
    }

    public static Delete createEdgeLabelIndexDelete(long edgeId, String label) {
	Delete labelIndexPut = new Delete(Bytes.toBytes(label));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId));
	return labelIndexPut;
    }

    public static Delete createEdgePropertyIndexDelete(long edgeId, String key) {
	Delete indexPut = new Delete(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId));
	return indexPut;
    }

}
