package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.time.Instant;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class TimestampColumnType implements ColumnTypeDefinition<Instant> {

    @Override
    public String getName() {
	return "TIMESTAMP";
    }

    @Override
    public Class<Instant> getJavaClass() {
	return Instant.class;
    }

    @Override
    public byte[] toBytes(Object value) {
	return Bytes.toBytes((Instant) value);
    }

    @Override
    public Instant fromBytes(byte[] value) {
	return Bytes.toInstant(value);
    }

}
