package com.puresoltechnologies.ductiledb.engine.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class CompactorIT extends AbstractColumnFamiliyEngineTest {

    private static String NAMESPACE = CompactorIT.class.getSimpleName();

    /**
     * This test was added, because of an issue in the first implementation
     * causing the graph database to crash.
     * 
     * @throws SchemaException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testRolloverOfEmptyCommitLog() throws InterruptedException, IOException {
	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testRolloverOfEmptyCommitLog",
		"testcf")) {
	    Key rowKey = Key.of(1l);

	    ColumnMap result = columnFamilyEngine.get(rowKey);
	    assertTrue(result.isEmpty());
	    assertFalse(columnFamilyEngine.getScanner(null, null).hasNext());

	    ColumnMap columnMap = new ColumnMap();
	    columnMap.put(Key.of(10l), ColumnValue.of(100l));
	    columnFamilyEngine.put(rowKey, columnMap);

	    result = columnFamilyEngine.get(rowKey);
	    assertEquals(columnMap, result);
	    assertTrue(columnFamilyEngine.getScanner(null, null).hasNext());

	    columnFamilyEngine.runCompaction();

	    result = columnFamilyEngine.get(rowKey);
	    assertEquals(columnMap, result);
	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertTrue(scanner.hasNext());

	    Thread.sleep(100l);
	    columnFamilyEngine.runCompaction();

	    result = columnFamilyEngine.get(rowKey);
	    assertEquals(columnMap, result);
	    assertTrue(columnFamilyEngine.getScanner(null, null).hasNext());

	    columnFamilyEngine.delete(rowKey);
	    result = columnFamilyEngine.get(rowKey);
	    assertTrue(result.isEmpty());
	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertFalse(scanner.hasNext());

	    columnFamilyEngine.runCompaction();

	    result = columnFamilyEngine.get(rowKey);
	    assertTrue(result.isEmpty());
	    assertFalse(columnFamilyEngine.getScanner(null, null).hasNext());

	    columnFamilyEngine.runCompaction();

	    result = columnFamilyEngine.get(rowKey);
	    assertTrue(result.isEmpty());
	    assertFalse(columnFamilyEngine.getScanner(null, null).hasNext());
	}
    }

}
