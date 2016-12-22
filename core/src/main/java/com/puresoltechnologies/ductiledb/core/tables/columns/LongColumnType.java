package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.engine.io.Bytes;

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
    public int getDataType() {
	return Types.BIGINT;
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
