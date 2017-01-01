package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.StatementImpl;

public class BoundStatementImpl extends StatementImpl implements BoundStatement {

    private final Map<Integer, Comparable<?>> placeholderValues = new HashMap<>();
    private final AbstractPreparedStatement preparedStatement;

    public BoundStatementImpl(AbstractPreparedStatement preparedStatement) {
	super(preparedStatement.getTableStore());
	this.preparedStatement = preparedStatement;
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	return preparedStatement.execute(placeholderValues);
    }

    @Override
    public BoundStatement setBoolean(int index, boolean value) {
	return this;
    }

    @Override
    public BoundStatement setByte(int index, byte value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setDate(int index, LocalDate value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setDateTime(int index, LocalDateTime value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setDouble(int index, double value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setInteger(int index, int value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setLong(int index, long value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setShort(int index, short value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setSingle(int index, float value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setTime(int index, LocalTime value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setTimestamp(int index, Instant value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setVarChar(int index, String value) {
	placeholderValues.put(index, value);
	return this;
    }

    @Override
    public BoundStatement setNull(int index) {
	placeholderValues.put(index, null);
	return this;
    }

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
