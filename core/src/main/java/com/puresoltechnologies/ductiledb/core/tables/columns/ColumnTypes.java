package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;

public enum ColumnTypes {

    BOOLEAN(new BooleanColumnType()), //
    BYTE(new ByteColumnType()), //
    DATE(new DateColumnType()), //
    DATETIME(new DateTimeColumnType()), //
    DOUBLE(new DoubleColumnType()), //
    INTEGER(new IntColumnType()), //
    LONG(new LongColumnType()), //
    SHORT(new ShortColumnType()), //
    SINGLE(new SingleColumnType()), //
    TIME(new TimeColumnType()), //
    TIMESTAMP(new TimestampColumnType()), //
    VARCHAR(new VarCharColumnType()),//
    ;

    private final ColumnType<?> type;

    private ColumnTypes(ColumnType<?> type) {
	this.type = type;
    }

    public ColumnType<?> getType() {
	return type;
    }
}
