package com.puresoltechnologies.ductiledb.core.tables.columns;

/**
 * This interface is implemented by all provided column types for tables part of
 * DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface ColumnTypeDefinition<Type> {

    public String getName();

    public Class<Type> getJavaClass();

    public byte[] toBytes(Object value);

    public Type fromBytes(byte[] value);

}
