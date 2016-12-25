package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

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
    public int getDataType() {
	return Types.BOOLEAN;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Boolean) value);
    }

    @Override
    public Boolean fromBytes(byte[] value) {
	return Bytes.toBoolean(value);
    }

}
