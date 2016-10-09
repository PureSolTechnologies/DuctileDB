package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class IntColumnType implements ColumnType<Integer> {

    @Override
    public Class<Integer> getJavaClass() {
	return Integer.class;
    }

    @Override
    public byte[] toBytes(Integer value) {
	return Bytes.toBytes(value);
    }

    @Override
    public Integer fromBytes(byte[] value) {
	return Bytes.toInt(value);
    }

}
