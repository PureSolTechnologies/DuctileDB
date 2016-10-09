package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class LongColumnType implements ColumnType<Long> {

    @Override
    public Class<Long> getJavaClass() {
	return Long.class;
    }

    @Override
    public byte[] toBytes(Long value) {
	return Bytes.toBytes(value);
    }

    @Override
    public Long fromBytes(byte[] value) {
	return Bytes.toLong(value);
    }

}
