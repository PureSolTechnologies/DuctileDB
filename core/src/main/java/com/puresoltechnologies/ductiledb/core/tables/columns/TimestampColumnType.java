package com.puresoltechnologies.ductiledb.core.tables.columns;

import java.time.Instant;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class TimestampColumnType implements ColumnType<Instant> {

    @Override
    public Class<Instant> getJavaClass() {
	return Instant.class;
    }

    @Override
    public byte[] toBytes(Instant value) {
	return Bytes.toBytes(value);
    }

    @Override
    public Instant fromBytes(byte[] value) {
	return Bytes.toInstant(value);
    }

}
