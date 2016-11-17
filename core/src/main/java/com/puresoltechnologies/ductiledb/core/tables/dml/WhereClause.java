package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.Result;

public class WhereClause<T extends Comparable<T>> {

    private final TableDefinition tableDefinition;
    private final ColumnDefinition<T> columnDefinition;
    private final CompareOperator operator;
    private final T value;

    public WhereClause(TableDefinition tableDefinition, ColumnDefinition<T> columnDefinition, CompareOperator operator,
	    T value) {
	super();
	this.tableDefinition = tableDefinition;
	this.columnDefinition = columnDefinition;
	this.operator = operator;
	this.value = value;
    }

    public ColumnDefinition<T> getColumnDefinition() {
	return columnDefinition;
    }

    public CompareOperator getOperator() {
	return operator;
    }

    public Object getValue() {
	return value;
    }

    public boolean matches(Result result) {
	TableRow row = TableRowCreator.create(tableDefinition, result);
	@SuppressWarnings("unchecked")
	T value = columnDefinition.getType().fromObject(row.get(columnDefinition.getName()));
	return operator.matches(this.value, value);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((columnDefinition == null) ? 0 : columnDefinition.hashCode());
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
	if (columnDefinition == null) {
	    if (other.columnDefinition != null)
		return false;
	} else if (!columnDefinition.equals(other.columnDefinition))
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
