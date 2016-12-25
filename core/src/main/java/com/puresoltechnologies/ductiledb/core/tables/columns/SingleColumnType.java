package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class SingleColumnType implements ColumnTypeDefinition<Float> {

    @Override
    public String getName() {
	return "SINGLE";
    }

    @Override
    public Class<Float> getJavaClass() {
	return Float.class;
    }

    @Override
    public int getDataType() {
	return Types.FLOAT;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Float) value);
    }

    @Override
    public Float fromBytes(byte[] value) {
	return Bytes.toFloat(value);
    }

}
