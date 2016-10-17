package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.time.LocalTime;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class TimeColumnType implements ColumnTypeDefinition<LocalTime> {

    @Override
    public String getName() {
	return "TIME";
    }

    @Override
    public Class<LocalTime> getJavaClass() {
	return LocalTime.class;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((LocalTime) value);
    }

    @Override
    public LocalTime fromBytes(byte[] value) {
	return Bytes.toLocalTime(value);
    }

}
