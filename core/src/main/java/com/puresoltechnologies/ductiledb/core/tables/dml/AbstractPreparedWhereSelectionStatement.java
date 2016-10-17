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

    private final Map<Integer, String> indexToName = new HashMap<>();
    private final Map<String, Integer> nameToIndex = new HashMap<>();
    private final Map<String, Object> selections = new HashMap<>();

    public AbstractPreparedWhereSelectionStatement(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public void addWherePlaceholder(String column, int index) {
	indexToName.put(index, column);
	nameToIndex.put(column, index);
    }

    @Override
    public void addWhereSelection(String column, Object value) {
	selections.put(column, value);
    }

    public final Map<String, Object> getSelections() {
	return selections;
    }

}
