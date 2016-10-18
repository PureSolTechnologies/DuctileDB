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

    private final Map<String, Object> selections = new HashMap<>();
    private final AbstractPreparedStatementImpl preparedStatement;

    public BoundStatementImpl(AbstractPreparedStatementImpl preparedStatement) {
	super();
	this.preparedStatement = preparedStatement;
	if (AbstractPreparedWhereSelectionStatement.class.isAssignableFrom(preparedStatement.getClass())) {
	    this.selections.putAll(((AbstractPreparedWhereSelectionStatement) preparedStatement).getSelections());
	} else {
	    this.selections.putAll(new HashMap<>());
	}
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	return preparedStatement.execute(selections);
    }

    @Override
    public BoundStatement setBoolean(int index, boolean value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setByte(int index, byte value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setDate(int index, LocalDate value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setDateTime(int index, LocalDateTime value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setDouble(int index, double value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setInteger(int index, int value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setLong(int index, long value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setShort(int index, short value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setSingle(int index, float value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setTime(int index, LocalTime value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setTimestamp(int index, Instant value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setVarChar(int index, String value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setNull(int index) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setBoolean(String name, boolean value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setByte(String name, byte value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setDate(String name, LocalDate value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setDateTime(String name, LocalDateTime value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setDouble(String name, double value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setInteger(String name, int value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setLong(String name, long value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setShort(String name, short value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setSingle(String name, float value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setTime(String name, LocalTime value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setTimestamp(String name, Instant value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setVarChar(String name, String value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public BoundStatement setNull(String name) {
	// TODO Auto-generated method stub
	return null;
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
