package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.time.LocalDate;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class DateColumnType implements ColumnType<LocalDate> {

    @Override
    public String getName() {
	return "DATE";
    }

    @Override
    public Class<LocalDate> getJavaClass() {
	return LocalDate.class;
    }

    @Override
    public byte[] toBytes(LocalDate value) {
	return Bytes.toBytes(value);
    }

    @Override
    public LocalDate fromBytes(byte[] value) {
	return Bytes.toLocalDate(value);
    }

}
