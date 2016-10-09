package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.time.LocalTime;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class TimeColumnType implements ColumnType<LocalTime> {

    @Override
    public Class<LocalTime> getJavaClass() {
	return LocalTime.class;
    }

    @Override
    public byte[] toBytes(LocalTime value) {
	return Bytes.toBytes(value);
    }

    @Override
    public LocalTime fromBytes(byte[] value) {
	return Bytes.toLocalTime(value);
    }

}
