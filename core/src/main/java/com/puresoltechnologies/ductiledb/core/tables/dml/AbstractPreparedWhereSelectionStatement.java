package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.ArrayList;
import java.util.List;

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

    private final List<WhereClause> whereClauses = new ArrayList<>();

    public AbstractPreparedWhereSelectionStatement(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public final void addWherePlaceholder(String columnFamily, String column, CompareOperator operator, int index) {
	WherePlaceholder value = new WherePlaceholder(index, columnFamily, column, operator);
	addPlaceholder(value);
    }

    @Override
    public final void addWhereSelection(String columnFamily, String column, CompareOperator operator, Object value) {
	whereClauses.add(new WhereClause(columnFamily, column, operator, value));
    }

    public final List<WhereClause> getSelections() {
	return whereClauses;
    }

}
