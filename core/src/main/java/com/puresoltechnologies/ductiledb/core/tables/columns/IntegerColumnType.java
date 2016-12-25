package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class IntegerColumnType implements ColumnTypeDefinition<Integer> {

    @Override
    public String getName() {
	return "INTEGER";
    }

    @Override
    public Class<Integer> getJavaClass() {
	return Integer.class;
    }

    @Override
    public int getDataType() {
	return Types.INTEGER;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Integer) value);
    }

    @Override
    public Integer fromBytes(byte[] value) {
	return Bytes.toInt(value);
    }

}
