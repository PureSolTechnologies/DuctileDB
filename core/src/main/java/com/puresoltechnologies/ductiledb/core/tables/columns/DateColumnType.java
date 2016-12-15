package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.sql.Types;
import java.time.LocalDate;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class DateColumnType implements ColumnTypeDefinition<LocalDate> {

    @Override
    public String getName() {
	return "DATE";
    }

    @Override
    public Class<LocalDate> getJavaClass() {
	return LocalDate.class;
    }

    @Override
    public int getDataType() {
	return Types.DATE;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((LocalDate) value);
    }

    @Override
    public LocalDate fromBytes(byte[] value) {
	return Bytes.toLocalDate(value);
    }

}
