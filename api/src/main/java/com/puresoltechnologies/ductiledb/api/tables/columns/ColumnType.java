package com.puresoltechnologies.ductiledb.api.tables.columns;

/**
 * This interface is implemented by all provided column types for tables part of
 * DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface ColumnType<Type> {

    public String getName();

    public Class<Type> getJavaClass();

    public byte[] toBytes(Type value);

    public Type fromBytes(byte[] value);

}
