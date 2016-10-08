package com.puresoltechnologies.ductiledb.core.tables;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.Namespace;

public class RelationalDuctileDBIT extends AbstractTableStoreTest {

    @Test
    public void testEmptyDatabase() {
	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CloseableIterable<Namespace> namespaces = ddl.getNamespaces();
	assertNotNull(namespaces);
	Iterator<Namespace> iterator = namespaces.iterator();
	assertNotNull(iterator);
	assertFalse(iterator.hasNext());
    }

}
