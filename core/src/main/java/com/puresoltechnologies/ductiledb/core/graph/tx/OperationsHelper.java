package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.io.Serializable;

import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;

public class OperationsHelper {

    /**
     * Private constructor to avoid instantiation.
     */
    private OperationsHelper() {
    }

    public static Put createVertexTypeIndexPut(long vertexId, String type) {
	Put typeIndexPut = new Put(Key.of(type));
	typeIndexPut.addColumn(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(vertexId)),
		ColumnValue.empty());
	return typeIndexPut;
    }

    public static Delete createVertexTypeIndexDelete(long vertexId, String type) {
	Delete typeIndexDelete = new Delete(Key.of(type));
	typeIndexDelete.addColumns(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(vertexId)));
	return typeIndexDelete;
    }

    public static Put createVertexPropertyIndexPut(long vertexId, String key, Serializable value) throws IOException {
	Put indexPut = new Put(Key.of(key));
	indexPut.addColumn(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(vertexId)),
		ColumnValue.of(Serializer.serializePropertyValue(value)));
	return indexPut;
    }

    public static Delete createVertexPropertyIndexDelete(long vertexId, String key) {
	Delete indexDelete = new Delete(Key.of(key));
	indexDelete.addColumns(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(vertexId)));
	return indexDelete;
    }

    public static Put createEdgeTypeIndexPut(long edgeId, String type) {
	Put typeIndexPut = new Put(Key.of(type));
	typeIndexPut.addColumn(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(edgeId)),
		ColumnValue.empty());
	return typeIndexPut;
    }

    public static Put createEdgePropertyIndexPut(long edgeId, String key, Serializable value) throws IOException {
	Put indexPut = new Put(Key.of(key));
	indexPut.addColumn(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(edgeId)),
		ColumnValue.of(Serializer.serializePropertyValue(value)));
	return indexPut;
    }

    public static Delete createEdgeTypeIndexDelete(long edgeId, String type) {
	Delete typeIndexDelete = new Delete(Key.of(type));
	typeIndexDelete.addColumns(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(edgeId)));
	return typeIndexDelete;
    }

    public static Delete createEdgePropertyIndexDelete(long edgeId, String key) {
	Delete indexDelete = new Delete(Key.of(key));
	indexDelete.addColumns(DatabaseColumnFamily.INDEX.getKey(), Key.of(IdEncoder.encodeRowId(edgeId)));
	return indexDelete;
    }

}
