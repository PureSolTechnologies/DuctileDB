package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class LongColumnType implements ColumnTypeDefinition<Long> {

    @Override
    public String getName() {
	return "LONG";
    }

    @Override
    public Class<Long> getJavaClass() {
	return Long.class;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Long) value);
    }

    @Override
    public Long fromBytes(byte[] value) {
	return Bytes.toLong(value);
    }

}
