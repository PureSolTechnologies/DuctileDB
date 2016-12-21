package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.blobstore.BlobStore;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;

public class DuctileDBFactoryIT extends AbstractDuctileDBTest {

    @Test
    public void testInitialization() {
	DuctileDB ductileDB = getDuctileDB();
	BlobStore blobStore = ductileDB.getBlobStore();
	assertNotNull("BLOB store available.", blobStore);
	GraphStore graph = ductileDB.getGraph();
	assertNotNull("Graph is available.", graph);
    }

}
