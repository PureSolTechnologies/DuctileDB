package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.engine.io.Bytes;

public class DoubleColumnType implements ColumnTypeDefinition<Byte> {

    @Override
    public String getName() {
	return "DOUBLE";
    }

    @Override
    public Class<Byte> getJavaClass() {
	return Byte.class;
    }

    @Override
    public int getDataType() {
	return Types.DOUBLE;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Double) value);
    }

    @Override
    public Byte fromBytes(byte[] value) {
	return Bytes.toByte(value);
    }

}
