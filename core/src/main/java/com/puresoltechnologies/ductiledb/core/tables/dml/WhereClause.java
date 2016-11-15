package com.puresoltechnologies.ductiledb.core.tables.dml;

public class WhereClause<T extends Comparable<T>> {

    private final String columnFamily;
    private final String column;
    private final CompareOperator operator;
    private final T value;

    public WhereClause(String columnFamily, String column, CompareOperator operator, T value) {
	super();
	this.columnFamily = columnFamily;
	this.column = column;
	this.operator = operator;
	this.value = value;
    }

    public String getColumnFamily() {
	return columnFamily;
    }

    public String getColumn() {
	return column;
    }

    public CompareOperator getOperator() {
	return operator;
    }

    public Object getValue() {
	return value;
    }

    public boolean matches(TableRow row) {
	T value = row.get(column);
	return operator.matches(this.value, value);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((column == null) ? 0 : column.hashCode());
	result = prime * result + ((columnFamily == null) ? 0 : columnFamily.hashCode());
	result = prime * result + ((operator == null) ? 0 : operator.hashCode());
	result = prime * result + ((value == null) ? 0 : value.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	WhereClause<?> other = (WhereClause<?>) obj;
	if (column == null) {
	    if (other.column != null)
		return false;
	} else if (!column.equals(other.column))
	    return false;
	if (columnFamily == null) {
	    if (other.columnFamily != null)
		return false;
	} else if (!columnFamily.equals(other.columnFamily))
	    return false;
	if (operator != other.operator)
	    return false;
	if (value == null) {
	    if (other.value != null)
		return false;
	} else if (!value.equals(other.value))
	    return false;
	return true;
    }

}
