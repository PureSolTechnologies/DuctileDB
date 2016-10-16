package com.puresoltechnologies.ductiledb.core.tables;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * This interface combines all method for data setters based on name.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DataSettersByIndex<T extends DataSettersByIndex<T>> {

    public T setBoolean(int index, boolean value);

    public T setByte(int index, byte value);

    public T setDate(int index, LocalDate value);

    public T setDateTime(int index, LocalDateTime value);

    public T setDouble(int index, double value);

    public T setInteger(int index, int value);

    public T setLong(int index, long value);

    public T setShort(int index, short value);

    public T setSingle(int index, float value);

    public T setTime(int index, LocalTime value);

    public T setTimestamp(int index, Instant value);

    public T setVarChar(int index, String value);

    public T setNull(int index);

}
