package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.CompoundKey;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;

public class TableRowCreator {

    public static TableRow create(TableDefinition tableDefinition, Result result) {
	Key rowKey = result.getRowKey();
	CompoundKey compoundKey = CompoundKey.of(rowKey);
	List<ColumnDefinition<?>> primaryKey = tableDefinition.getPrimaryKey();
	if (compoundKey.getPartNum() != primaryKey.size()) {
	    throw new IllegalArgumentException("The number of found key parts " + compoundKey.getPartNum()
		    + " do not match the definition of the primary key which contains " + primaryKey.size() + " part.");
	}
	TableRowImpl tableRow = new TableRowImpl(tableDefinition, rowKey);
	for (int partId = 0; partId < primaryKey.size(); ++partId) {
	    ColumnDefinition<?> columnDefinition = primaryKey.get(partId);
	    tableRow.add(columnDefinition.getName(), compoundKey.getPart(partId));
	}
	for (Key family : result.getFamilies()) {
	    NavigableMap<Key, ColumnValue> familyMap = result.getFamilyMap(family);
	    for (Entry<Key, ColumnValue> entry : familyMap.entrySet()) {
		String columnName = entry.getKey().toString();
		ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(columnName);
		tableRow.add(columnName, entry.getValue().getBytes());
	    }
	}
	return tableRow;
    }

}
