package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class ShortColumnType implements ColumnType<Short> {

    @Override
    public Class<Short> getJavaClass() {
	return Short.class;
    }

    @Override
    public byte[] toBytes(Short value) {
	return Bytes.toBytes(value);
    }

    @Override
    public Short fromBytes(byte[] value) {
	return Bytes.toShort(value);
    }

}
