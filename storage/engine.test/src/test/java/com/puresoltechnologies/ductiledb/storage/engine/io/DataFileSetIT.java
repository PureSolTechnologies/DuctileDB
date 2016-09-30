package com.puresoltechnologies.ductiledb.storage.engine.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileSetIT extends AbstractColumnFamiliyEngineTest {

    private static String NAMESPACE = DataFileSetIT.class.getSimpleName();

    @Test
    public void testIndexIterator() throws SchemaException, StorageException, IOException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testIndexIterator",
		"testcf")) {
	    Storage storage = getStorage();

	    columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(25 * 1024);

	    for (long i = 1; i <= 1000; ++i) {
		ColumnMap columnMap = new ColumnMap();
		columnMap.put(Bytes.toBytes(i * 10), Bytes.toBytes(i * 100));
		columnFamilyEngine.put(Bytes.toBytes(i), columnMap);
	    }
	    columnFamilyEngine.runCompaction();

	    DataFileSet dataFileSet = new DataFileSet(storage, columnFamilyEngine.getDirectory());

	    IndexIterator indexIterator = dataFileSet.getIndexIterator(new Key(Bytes.toBytes(100l)),
		    new Key(Bytes.toBytes(900l)));
	    assertNotNull(indexIterator);

	    long current = 100l;
	    while (indexIterator.hasNext()) {
		IndexEntry startResult = indexIterator.next();
		long l = Bytes.toLong(startResult.getRowKey().getKey());
		assertEquals(current, l);
		++current;
	    }
	    assertEquals(901, current);

	}
    }

}
