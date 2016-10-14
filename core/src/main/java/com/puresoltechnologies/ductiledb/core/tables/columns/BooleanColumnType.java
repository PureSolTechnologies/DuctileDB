package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class BooleanColumnType implements ColumnTypeDefinition<Boolean> {

    @Override
    public String getName() {
	return "BOOLEAN";
    }

    @Override
    public Class<Boolean> getJavaClass() {
	return Boolean.class;
    }

    @Override
    public byte[] toBytes(Boolean value) {
	return Bytes.toBytes(value);
    }

    @Override
    public Boolean fromBytes(byte[] value) {
	return Bytes.toBoolean(value);
    }

}
