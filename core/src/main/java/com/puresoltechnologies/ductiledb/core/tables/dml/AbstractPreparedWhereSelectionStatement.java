package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;

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

    private final Map<Integer, WherePlaceholder> indexToName = new HashMap<>();
    private final Map<String, WherePlaceholder> nameToIndex = new HashMap<>();
    private final Map<String, WhereClause> selections = new HashMap<>();

    public AbstractPreparedWhereSelectionStatement(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public void addWherePlaceholder(String columnFamily, String column, CompareOperator operator, int index) {
	indexToName.put(index, new WherePlaceholder(columnFamily, column, operator, index));
	nameToIndex.put(column, new WherePlaceholder(columnFamily, column, operator, index));
    }

    @Override
    public void addWhereSelection(String columnFamily, String column, CompareOperator operator, Object value) {
	selections.put(column, new WhereClause(columnFamily, column, operator, value));
    }

    public final Map<String, WhereClause> getSelections() {
	return selections;
    }

}
