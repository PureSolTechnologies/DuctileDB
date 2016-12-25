package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;

import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class ShortColumnType implements ColumnTypeDefinition<Short> {

    @Override
    public String getName() {
	return "SHORT";
    }

    @Override
    public Class<Short> getJavaClass() {
	return Short.class;
    }

    @Override
    public int getDataType() {
	return Types.SMALLINT;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Short) value);
    }

    @Override
    public Short fromBytes(byte[] value) {
	return Bytes.toShort(value);
    }

}
