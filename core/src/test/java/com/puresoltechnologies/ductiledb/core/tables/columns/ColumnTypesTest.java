package com.puresoltechnologies.ductiledb.core.tables.columns;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ColumnTypesTest {

    @Test
    public void checkNames() {
	for (ColumnTypes type : ColumnTypes.values()) {
	    assertEquals(type.name(), type.getType().getName());
	}
    }

}
