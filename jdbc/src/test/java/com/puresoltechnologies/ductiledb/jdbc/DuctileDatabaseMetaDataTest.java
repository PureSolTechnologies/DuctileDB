package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.mockito.Mockito;

public class DuctileDatabaseMetaDataTest {

    @Test
    public void test() {
	DuctileConnection connection = Mockito.mock(DuctileConnection.class);
	DuctileDatabaseMetaData metaData = new DuctileDatabaseMetaData(connection);
	assertNotNull(metaData);
    }
}
