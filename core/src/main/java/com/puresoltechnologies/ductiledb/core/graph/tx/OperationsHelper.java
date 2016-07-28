package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.io.Serializable;

import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class OperationsHelper {

    /**
     * Private constructor to avoid instantiation.
     */
    private OperationsHelper() {
    }

    public static Put createVertexTypeIndexPut(long vertexId, String type) {
	Put typeIndexPut = new Put(Bytes.toBytes(type));
	typeIndexPut.addColumn(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(vertexId), new byte[0]);
	return typeIndexPut;
    }

    public static Delete createVertexTypeIndexDelete(long vertexId, String type) {
	Delete typeIndexDelete = new Delete(Bytes.toBytes(type));
	typeIndexDelete.addColumns(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(vertexId));
	return typeIndexDelete;
    }

    public static Put createVertexPropertyIndexPut(long vertexId, String key, Serializable value) throws IOException {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(vertexId),
		Serializer.serializePropertyValue(value));
	return indexPut;
    }

    public static Delete createVertexPropertyIndexDelete(long vertexId, String key) {
	Delete indexDelete = new Delete(Bytes.toBytes(key));
	indexDelete.addColumns(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(vertexId));
	return indexDelete;
    }

    public static Put createEdgeTypeIndexPut(long edgeId, String type) {
	Put typeIndexPut = new Put(Bytes.toBytes(type));
	typeIndexPut.addColumn(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(edgeId), new byte[0]);
	return typeIndexPut;
    }

    public static Put createEdgePropertyIndexPut(long edgeId, String key, Serializable value) throws IOException {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(edgeId),
		Serializer.serializePropertyValue(value));
	return indexPut;
    }

    public static Delete createEdgeTypeIndexDelete(long edgeId, String type) {
	Delete typeIndexDelete = new Delete(Bytes.toBytes(type));
	typeIndexDelete.addColumns(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(edgeId));
	return typeIndexDelete;
    }

    public static Delete createEdgePropertyIndexDelete(long edgeId, String key) {
	Delete indexDelete = new Delete(Bytes.toBytes(key));
	indexDelete.addColumns(HBaseColumnFamily.INDEX.getNameBytes(), IdEncoder.encodeRowId(edgeId));
	return indexDelete;
    }

}
