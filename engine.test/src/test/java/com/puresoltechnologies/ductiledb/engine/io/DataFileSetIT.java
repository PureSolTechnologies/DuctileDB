package com.puresoltechnologies.ductiledb.engine.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.IndexEntry;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.IndexIterator;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.engine.io.DataFileSet;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
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
		columnMap.put(Key.of(i * 10), ColumnValue.of(i * 100));
		columnFamilyEngine.put(Key.of(i), columnMap);
	    }
	    columnFamilyEngine.runCompaction();

	    DataFileSet dataFileSet = new DataFileSet(storage, columnFamilyEngine.getDirectory());

	    IndexIterator indexIterator = dataFileSet.getIndexIterator(Key.of(100l), Key.of(900l));
	    assertNotNull(indexIterator);

	    long current = 100l;
	    while (indexIterator.hasNext()) {
		IndexEntry startResult = indexIterator.next();
		long l = Bytes.toLong(startResult.getRowKey().getBytes());
		assertEquals(current, l);
		++current;
	    }
	    assertEquals(901, current);

	}
    }

}
