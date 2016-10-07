package com.puresoltechnologies.ductiledb.core.rdbms;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class AbstractDuctileDBRdbmsTest extends AbstractDuctileDBTest {

    private static DuctileDB ductileDB = null;
    private static TableStoreImpl rdbms = null;

    @BeforeClass
    public static void connect() throws IOException, SchemaException {
	ductileDB = getDuctileDB();
	rdbms = (TableStoreImpl) ductileDB.getTableStore();
	// Normally meaningless, but we do nevertheless, if tests change...
	DuctileDBTestHelper.removeRDBMS(rdbms);
	DuctileDBHealthCheck.runCheckForEmpty(rdbms);

	String version = BuildInformation.getVersion();
	if (!version.startsWith("${")) {
	    // TODO!!!
	    // assertEquals("Schema version is wrong.", version,
	    // rdbms.getVersion().toString());
	}
    }

    @AfterClass
    public static void disconnect() throws IOException {
	if (rdbms != null) {
	    rdbms.close();
	    rdbms = null;
	}
    }

    @Before
    public final void cleanup() throws IOException, StorageException {
	DuctileDBTestHelper.removeRDBMS(rdbms);
	rdbms.runCompaction();
	DuctileDBHealthCheck.runCheckForEmpty(rdbms);
    }

    protected static TableStoreImpl getRDBMS() {
	return rdbms;
    }
}
