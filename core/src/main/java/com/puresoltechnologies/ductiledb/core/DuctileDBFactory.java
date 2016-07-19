package com.puresoltechnologies.ductiledb.core;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

/**
 * This is the central factory to connect to DuctileDB. The primary goal is to
 * have a simple point of access, because DuctileDB needs some information to
 * connect to Hadoop and HBase.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBFactory {

    public static Map<String, String> createConfiguration() throws FileNotFoundException {
	return new HashMap<>();
    }

    public static DuctileDB connect(DuctileDBConfiguration configuration) throws StorageException, SchemaException {
	BlobStoreImpl blobStore = new BlobStoreImpl(configuration);
	DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(blobStore, configuration);
	return new DuctileDBImpl(blobStore, graph);
    }

}
