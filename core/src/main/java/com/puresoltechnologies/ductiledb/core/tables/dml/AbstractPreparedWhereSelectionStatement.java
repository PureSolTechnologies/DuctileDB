package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

/**
 * This abstract class handles the where selection clauses and the index best
 * suitable for the later data retrieval.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public abstract class AbstractPreparedWhereSelectionStatement extends AbstractPreparedDMLStatement
	implements PreparedWhereSelectionStatement {

    private final Set<WhereClause<?>> whereClauses = new HashSet<>();

    public AbstractPreparedWhereSelectionStatement(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public final PreparedWhereSelectionStatement addWherePlaceholder(String columnFamily, String column,
	    CompareOperator operator, int index) {
	WherePlaceholder value = new WherePlaceholder(index, columnFamily, column, operator);
	addPlaceholder(value);
	return this;
    }

    @Override
    public final <T extends Comparable<T>> PreparedWhereSelectionStatement addWhereSelection(
	    ColumnDefinition<T> columnDefinition, CompareOperator operator, T value) {
	addWhereSelection(new WhereClause<>(getTableDefinition(), columnDefinition, operator, value));
	return this;
    }

    @Override
    public <T extends Comparable<T>> PreparedWhereSelectionStatement addWhereSelection(String column,
	    CompareOperator operator, T value) {
	TableDefinition tableDefinition = getTableDefinition();
	ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(column);
	addWhereSelection(new WhereClause(tableDefinition, columnDefinition, operator, value));
	return this;
    }

    @Override
    public <T extends Comparable<T>> PreparedWhereSelectionStatement addWhereSelection(WhereClause<T> selection) {
	whereClauses.add(selection);
	return this;
    }

    @Override
    public PreparedWhereSelectionStatement addWhereSelections(Collection<WhereClause<?>> selections) {
	for (WhereClause<?> whereClause : selections) {
	    whereClauses.add(whereClause);
	}
	return this;
    }

    /**
     * This method returns the static selections, because placeholders cannot be
     * taken into account.
     * 
     * @return
     */
    public final Set<WhereClause<?>> getStaticSelections() {
	return whereClauses;
    }

    /**
     * This method returns the static selections, because placeholders cannot be
     * taken into account.
     * 
     * @return
     */
    public final Set<WhereClause<?>> getSelections(Map<Integer, Comparable<?>> placeholderValues) {
	TableDefinition tableDefinition = getTableDefinition();
	Set<WhereClause<?>> whereClauses = new HashSet<>(this.whereClauses);
	for (Entry<Integer, Comparable<?>> placeholderValueEntry : placeholderValues.entrySet()) {
	    Placeholder placeholder = getPlaceholder(placeholderValueEntry.getKey());
	    if (WherePlaceholder.class.isAssignableFrom(placeholder.getClass())) {
		WherePlaceholder wherePlaceholder = (WherePlaceholder) placeholder;
		Comparable<?> value = placeholderValueEntry.getValue();
		whereClauses.add(new WhereClause(tableDefinition,
			tableDefinition.getColumnDefinition(wherePlaceholder.getColumn()),
			wherePlaceholder.getOperator(), value));
	    }
	}
	return whereClauses;
    }

}
