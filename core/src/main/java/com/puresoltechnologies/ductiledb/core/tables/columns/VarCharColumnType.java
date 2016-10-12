package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class VarCharColumnType implements ColumnType<String> {

    @Override
    public String getName() {
	return "VARCHAR";
    }

    @Override
    public Class<String> getJavaClass() {
	return String.class;
    }

    @Override
    public byte[] toBytes(String value) {
	return Bytes.toBytes(value);
    }

    @Override
    public String fromBytes(byte[] value) {
	return Bytes.toString(value);
    }

}
