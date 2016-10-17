package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * This interface combines all method for data setters based on index.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DataSettersByName<T extends DataSettersByName<T>> {

    public T setBoolean(String name, boolean value);

    public T setByte(String name, byte value);

    public T setDate(String name, LocalDate value);

    public T setDateTime(String name, LocalDateTime value);

    public T setDouble(String name, double value);

    public T setInteger(String name, int value);

    public T setLong(String name, long value);

    public T setShort(String name, short value);

    public T setSingle(String name, float value);

    public T setTime(String name, LocalTime value);

    public T setTimestamp(String name, Instant value);

    public T setVarChar(String name, String value);

    public T setNull(String name);

}
