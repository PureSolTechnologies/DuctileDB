package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.BoundStatementImpl;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;

public class BoundDMLStatementImpl extends BoundStatementImpl implements BoundDMLStatement {

    private final Map<Integer, Comparable<?>> placeholderValues = new HashMap<>();
    private final AbstractPreparedDMLStatement preparedStatement;

    public BoundDMLStatementImpl(AbstractPreparedDMLStatement preparedStatement) {
	super(preparedStatement);
	this.preparedStatement = preparedStatement;
    }

    @Override
    public TableRowIterable execute(TableStore tableStore) throws ExecutionException {
	return preparedStatement.execute(tableStore, placeholderValues);
    }

    @Override
    public BoundDMLStatement setBoolean(int index, boolean value) {
	return this;
    }

    @Override
    public BoundDMLStatement setByte(int index, byte value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setDate(int index, LocalDate value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setDateTime(int index, LocalDateTime value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setDouble(int index, double value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setInteger(int index, int value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setLong(int index, long value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setShort(int index, short value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setSingle(int index, float value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setTime(int index, LocalTime value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setTimestamp(int index, Instant value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setVarChar(int index, String value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundDMLStatement setNull(int index) {
	placeholderValues.put(index, null);
	return this;
    }

    @Override
    public BoundDMLStatement setBoolean(String name, boolean value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setByte(String name, byte value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setDate(String name, LocalDate value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setDateTime(String name, LocalDateTime value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setDouble(String name, double value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setInteger(String name, int value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setLong(String name, long value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setShort(String name, short value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setSingle(String name, float value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setTime(String name, LocalTime value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setTimestamp(String name, Instant value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setVarChar(String name, String value) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), value);
	return this;
    }

    @Override
    public BoundDMLStatement setNull(String name) {
	placeholderValues.put(preparedStatement.getPlaceholderIndex(name), null);
	return this;
    }

    @Override
    public void set(int index, Object value) {
	Class<? extends Object> clazz = value.getClass();
	if (Boolean.class.equals(clazz)) {
	    setBoolean(index, (boolean) value);
	} else if (Byte.class.equals(clazz)) {
	    setByte(index, (byte) value);
	} else if (Short.class.equals(clazz)) {
	    setShort(index, (short) value);
	} else if (Integer.class.equals(clazz)) {
	    setInteger(index, (int) value);
	} else if (Long.class.equals(clazz)) {
	    setLong(index, (long) value);
	} else if (Float.class.equals(clazz)) {
	    setSingle(index, (float) value);
	} else if (Long.class.equals(clazz)) {
	    setDouble(index, (double) value);
	} else if (String.class.equals(clazz)) {
	    setVarChar(index, (String) value);
	} else if (LocalDate.class.equals(clazz)) {
	    setDate(index, (LocalDate) value);
	} else if (LocalTime.class.equals(clazz)) {
	    setTime(index, (LocalTime) value);
	} else if (LocalDateTime.class.equals(clazz)) {
	    setDateTime(index, (LocalDateTime) value);
	} else if (Instant.class.equals(clazz)) {
	    setTimestamp(index, (Instant) value);
	} else {
	    throw new IllegalArgumentException("Values of type '" + clazz.getName() + "' are not supported.");
	}
    }

}
