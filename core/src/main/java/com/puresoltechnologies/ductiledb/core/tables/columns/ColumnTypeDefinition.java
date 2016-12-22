package com.puresoltechnologies.ductiledb.core.tables.columns;

import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;

/**
 * This interface is implemented by all provided column types for tables part of
 * DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface ColumnTypeDefinition<Type> {

    public String getName();

    public Class<Type> getJavaClass();

    public int getDataType();

    public byte[] toBytes(Object value);

    public Type fromBytes(byte[] value);

    public default ColumnValue toColumnValue(Object value) {
	return ColumnValue.of(toBytes(value));
    }

    public default Type fromColumnValue(ColumnValue value) {
	return fromBytes(value.getBytes());
    }

    public default Type fromObject(Object value) {
	@SuppressWarnings("unchecked")
	Type t = (Type) value;
	return t;
    }
}
