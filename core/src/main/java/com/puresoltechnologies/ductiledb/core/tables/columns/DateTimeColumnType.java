package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.time.LocalDateTime;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class DateTimeColumnType implements ColumnTypeDefinition<LocalDateTime> {

    @Override
    public String getName() {
	return "DATETIME";
    }

    @Override
    public Class<LocalDateTime> getJavaClass() {
	return LocalDateTime.class;
    }

    @Override
    public byte[] toBytes(LocalDateTime value) {
	return Bytes.toBytes(value);
    }

    @Override
    public LocalDateTime fromBytes(byte[] value) {
	short year = Bytes.toShort(value, 0);
	byte month = Bytes.toByte(value, 2);
	byte day = Bytes.toByte(value, 3);
	byte hour = Bytes.toByte(value, 4);
	byte minute = Bytes.toByte(value, 5);
	byte second = Bytes.toByte(value, 6);
	int nano = Bytes.toInt(value, 7);
	return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }

}
