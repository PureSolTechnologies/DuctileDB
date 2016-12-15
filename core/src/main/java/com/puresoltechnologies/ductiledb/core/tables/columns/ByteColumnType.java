package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class ByteColumnType implements ColumnTypeDefinition<Byte> {

    @Override
    public String getName() {
	return "BYTE";
    }

    @Override
    public Class<Byte> getJavaClass() {
	return Byte.class;
    }

    @Override
    public int getDataType() {
	return Types.TINYINT;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Byte) value);
    }

    @Override
    public Byte fromBytes(byte[] value) {
	return Bytes.toByte(value);
    }

}
