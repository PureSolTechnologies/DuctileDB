package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;

public class DuctileDBFactoryIT extends AbstractDuctileDBTest {

    @Test
    public void testInitialization() {
	DuctileDB ductileDB = getDuctileDB();
	BlobStore blobStore = ductileDB.getBlobStore();
	assertNotNull("BLOB store available.", blobStore);
	DuctileDBGraph graph = ductileDB.getGraph();
	assertNotNull("Grapg available.", graph);
    }

}
