package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.INDEX_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.utils.Serializer;

public class OperationsHelper {

    /**
     * Private constructor to avoid instantiation.
     */
    private OperationsHelper() {
    }

    public static Put createVertexTypeIndexPut(long vertexId, String type) {
	Put typeIndexPut = new Put(Bytes.toBytes(type));
	typeIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId), new byte[0]);
	return typeIndexPut;
    }

    public static Delete createVertexTypeIndexDelete(long vertexId, String type) {
	Delete typeIndexPut = new Delete(Bytes.toBytes(type));
	typeIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId));
	return typeIndexPut;
    }

    public static Put createVertexPropertyIndexPut(long vertexId, String key, Serializable value) throws IOException {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId),
		Serializer.serializePropertyValue(value));
	return indexPut;
    }

    public static Delete createVertexPropertyIndexDelete(long vertexId, String key) {
	Delete indexPut = new Delete(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId));
	return indexPut;
    }

    public static Put createEdgeTypeIndexPut(long edgeId, String type) {
	Put typeIndexPut = new Put(Bytes.toBytes(type));
	typeIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId), new byte[0]);
	return typeIndexPut;
    }

    public static Put createEdgePropertyIndexPut(long edgeId, String key, Serializable value) throws IOException {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId),
		Serializer.serializePropertyValue(value));
	return indexPut;
    }

    public static Delete createEdgeTypeIndexDelete(long edgeId, String type) {
	Delete typeIndexPut = new Delete(Bytes.toBytes(type));
	typeIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId));
	return typeIndexPut;
    }

    public static Delete createEdgePropertyIndexDelete(long edgeId, String key) {
	Delete indexPut = new Delete(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId));
	return indexPut;
    }

}
