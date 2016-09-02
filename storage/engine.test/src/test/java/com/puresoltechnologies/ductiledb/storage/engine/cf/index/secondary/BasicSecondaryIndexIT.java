package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.AbstractDatabaseEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineIT;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class BasicSecondaryIndexIT extends AbstractDatabaseEngineTest {

    private static final String NAMESPACE = DatabaseEngineIT.class.getSimpleName();

    @Test
    public void testSecondaryIndexCreateGetDelete() throws SchemaException, StorageException {
	DatabaseEngineImpl engine = getEngine();

	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testSecondaryIndexCreateGetDelete");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		Bytes.toBytes("testcf"));

	ColumnKeySet columnKeySet = new ColumnKeySet();
	SecondaryIndexDescriptor indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST", columnFamilyDescriptor,
		columnKeySet);
	schemaManager.createIndex(columnFamilyDescriptor, indexDescriptor);

	Iterable<SecondaryIndexDescriptor> indizes = schemaManager.getIndizes(columnFamilyDescriptor);
	Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	assertTrue(iterator.hasNext());
	assertEquals(indexDescriptor, iterator.next());
	assertFalse(iterator.hasNext());

	SecondaryIndexDescriptor index = schemaManager.getIndex("IDX_TEST", columnFamilyDescriptor);
	assertEquals(indexDescriptor, index);

	schemaManager.dropIndex("IDX_TEST", columnFamilyDescriptor);

	indizes = schemaManager.getIndizes(columnFamilyDescriptor);
	iterator = indizes.iterator();
	assertFalse(iterator.hasNext());

	index = schemaManager.getIndex("IDX_TEST", columnFamilyDescriptor);
	assertNull(index);
    }
}
