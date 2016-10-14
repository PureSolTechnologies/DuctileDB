package com.puresoltechnologies.ductiledb.core.tables.columns;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;

public class ColumnTypesTest {

    @Test
    public void checkNames() {
	for (ColumnType type : ColumnType.values()) {
	    assertEquals(type.name(), type.getType().getName());
	}
    }

}
