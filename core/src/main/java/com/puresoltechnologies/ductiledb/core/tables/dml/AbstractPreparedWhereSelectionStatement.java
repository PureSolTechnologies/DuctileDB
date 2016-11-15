package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

/**
 * This abstract class handles the where selection clauses and the index best
 * suitable for the later data retrieval.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public abstract class AbstractPreparedWhereSelectionStatement extends AbstractPreparedStatementImpl
	implements PreparedWhereSelectionStatement {

    private final Set<WhereClause<?>> whereClauses = new HashSet<>();

    public AbstractPreparedWhereSelectionStatement(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public final void addWherePlaceholder(String columnFamily, String column, CompareOperator operator, int index) {
	WherePlaceholder value = new WherePlaceholder(index, columnFamily, column, operator);
	addPlaceholder(value);
    }

    @Override
    public final <T extends Comparable<T>> void addWhereSelection(String columnFamily, String column,
	    CompareOperator operator, T value) {
	addWhereSelection(new WhereClause<>(columnFamily, column, operator, value));
    }

    @Override
    public <T extends Comparable<T>> void addWhereSelection(WhereClause<T> selection) {
	whereClauses.add(selection);
    }

    @Override
    public void addWhereSelections(Collection<WhereClause<?>> selections) {
	for (WhereClause<?> whereClause : selections) {
	    whereClauses.add(whereClause);
	}
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
	Set<WhereClause<?>> whereClauses = new HashSet<>(this.whereClauses);
	for (Entry<Integer, Comparable<?>> placeholderValueEntry : placeholderValues.entrySet()) {
	    Placeholder placeholder = getPlaceholder(placeholderValueEntry.getKey());
	    if (WherePlaceholder.class.isAssignableFrom(placeholder.getClass())) {
		WherePlaceholder wherePlaceholder = (WherePlaceholder) placeholder;
		Comparable<?> value = placeholderValueEntry.getValue();
		whereClauses.add(new WhereClause(wherePlaceholder.getColumnFamily(), wherePlaceholder.getColumn(),
			wherePlaceholder.getOperator(), value));
	    }
	}
	return whereClauses;
    }

}
