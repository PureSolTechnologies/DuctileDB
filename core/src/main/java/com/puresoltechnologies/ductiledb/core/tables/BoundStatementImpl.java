package com.puresoltechnologies.ductiledb.core.tables;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class BoundStatementImpl extends StatementImpl implements BoundStatement {

    private final PreparedStatementImpl preparedStatement;

    public BoundStatementImpl(PreparedStatementImpl preparedStatement) {
	super();
	this.preparedStatement = preparedStatement;
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	return preparedStatement.execute();
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

}
