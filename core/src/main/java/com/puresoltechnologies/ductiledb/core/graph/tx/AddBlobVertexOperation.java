package com.puresoltechnologies.ductiledb.core.graph.tx;

import static com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema.DUCTILEDB_ID_PROPERTY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.puresoltechnologies.commons.misc.hash.HashId;
import com.puresoltechnologies.commons.misc.hash.HashIdCreatorInputStream;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.engine.Put;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class AddBlobVertexOperation extends AbstractTxOperation {

    private final HashId hashId;
    private final File blobFile;
    private final Set<String> types;
    private final Map<String, Object> properties;
    private final DuctileDBCacheVertex vertex;

    public AddBlobVertexOperation(DuctileDBTransactionImpl transaction, long vertexId, InputStream blobContent,
	    Set<String> types, Map<String, Object> properties) {
	super(transaction);
	try {
	    this.types = Collections.unmodifiableSet(types);
	    this.types.add(GraphStore.BLOB_TYPE_NAME);
	    this.properties = Collections.unmodifiableMap(properties);
	    this.vertex = new DuctileDBCacheVertex(transaction, vertexId, types, properties, new ArrayList<>());
	    blobFile = File.createTempFile("add-vertex-" + vertex.getId(), ".blob");
	    blobFile.deleteOnExit();
	    try (CountingInputStream countingInputStream = new CountingInputStream(blobContent); //
		    HashIdCreatorInputStream hashIdStream = new HashIdCreatorInputStream(countingInputStream); //
		    FileOutputStream outputStream = new FileOutputStream(this.blobFile)) {
		ByteStreams.copy(hashIdStream, outputStream);
		hashId = hashIdStream.getHashId();
		long size = countingInputStream.getCount();
		properties.put(GraphStore.BLOB_HASHID_PROPERTY_NAME, hashId.toString());
		properties.put(GraphStore.BLOB_SIZE_PROPERTY_NAME, size);
	    }
	} catch (IOException e) {
	    throw new DuctileDBTransactionException("Could not create temporary file.", e);
	}
    }

    @Override
    protected void finalize() throws Throwable {
	if (blobFile.exists()) {
	    blobFile.delete();
	}
	super.finalize();
    }

    public long getVertexId() {
	return vertex.getId();
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.setCachedVertex(vertex);
    }

    @Override
    public void rollbackInternally() {
	blobFile.delete();
    }

    @Override
    public void perform() throws IOException {
	try (FileInputStream stream = new FileInputStream(blobFile)) {
	    getTransaction().getBlobStore().storeBlob(hashId, stream);
	    long vertexId = vertex.getId();
	    byte[] id = IdEncoder.encodeRowId(vertexId);
	    Put put = new Put(Key.of(id));
	    List<Put> typeIndex = new ArrayList<>();
	    for (String type : types) {
		put.addColumn(DatabaseColumnFamily.TYPES.getKey(), Key.of(type), ColumnValue.empty());
		typeIndex.add(OperationsHelper.createVertexTypeIndexPut(vertexId, type));
	    }
	    List<Put> propertyIndex = new ArrayList<>();
	    put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(DUCTILEDB_ID_PROPERTY),
		    ColumnValue.of(Serializer.serializePropertyValue(vertexId)));
	    put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY),
		    ColumnValue.of(Serializer.serializePropertyValue(new Date())));
	    for (Entry<String, Object> property : properties.entrySet()) {
		String key = property.getKey();
		Object value = property.getValue();
		put.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(key),
			ColumnValue.of(Serializer.serializePropertyValue((Serializable) value)));
		propertyIndex.add(OperationsHelper.createVertexPropertyIndexPut(vertexId, property.getKey(),
			(Serializable) property.getValue()));
	    }
	    put(DatabaseTable.VERTICES.getName(), put);
	    put(DatabaseTable.VERTEX_TYPES.getName(), typeIndex);
	    put(DatabaseTable.VERTEX_PROPERTIES.getName(), propertyIndex);
	} finally {
	    blobFile.delete();
	}
    }
}