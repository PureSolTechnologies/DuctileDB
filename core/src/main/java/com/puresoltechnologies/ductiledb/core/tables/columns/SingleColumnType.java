package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

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
    public byte[] toBytes(Float value) {
	return Bytes.toBytes(value);
    }

    @Override
    public Float fromBytes(byte[] value) {
	return Bytes.toFloat(value);
    }

}
