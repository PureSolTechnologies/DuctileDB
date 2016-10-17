package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

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
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Double) value);
    }

    @Override
    public Byte fromBytes(byte[] value) {
	return Bytes.toByte(value);
    }

}
