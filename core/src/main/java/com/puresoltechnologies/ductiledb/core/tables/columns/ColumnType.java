package com.puresoltechnologies.ductiledb.core.tables.columns;

public enum ColumnType {

    BOOLEAN(new BooleanColumnType()), //
    BYTE(new ByteColumnType()), //
    DATE(new DateColumnType()), //
    DATETIME(new DateTimeColumnType()), //
    DOUBLE(new DoubleColumnType()), //
    INTEGER(new IntegerColumnType()), //
    LONG(new LongColumnType()), //
    SHORT(new ShortColumnType()), //
    SINGLE(new SingleColumnType()), //
    TIME(new TimeColumnType()), //
    TIMESTAMP(new TimestampColumnType()), //
    VARCHAR(new VarCharColumnType()),//
    ;

    private final ColumnTypeDefinition<?> type;

    private ColumnType(ColumnTypeDefinition<?> type) {
	this.type = type;
    }

    public ColumnTypeDefinition<?> getType() {
	return type;
    }

}
