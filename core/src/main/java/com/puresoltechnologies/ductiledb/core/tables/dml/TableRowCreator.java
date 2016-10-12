package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.api.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.api.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRow;
import com.puresoltechnologies.ductiledb.storage.engine.CompoundKey;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class TableRowCreator {

    public static TableRow create(TableDefinition tableDefinition, Result result) {
	byte[] rowKey = result.getRowKey();
	CompoundKey compoundKey = CompoundKey.of(rowKey);
	List<ColumnDefinition<?>> primaryKey = tableDefinition.getPrimaryKey();
	if (compoundKey.getPartNum() != primaryKey.size()) {
	    throw new IllegalArgumentException("The number of found key parts " + compoundKey.getPartNum()
		    + " do not match the definition of the primary key which contains " + primaryKey.size() + " part.");
	}
	TableRow tableRow = new TableRow();
	for (int partId = 0; partId < primaryKey.size(); ++partId) {
	    ColumnDefinition<?> columnDefinition = primaryKey.get(partId);
	    tableRow.add(columnDefinition.getName(), compoundKey.getPart(partId));
	}
	for (byte[] family : result.getFamilies()) {
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family);
	    for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
		String columnName = Bytes.toString(entry.getKey());
		ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(columnName);
		tableRow.add(columnName, entry.getValue());
	    }
	}
	return tableRow;
    }

}
