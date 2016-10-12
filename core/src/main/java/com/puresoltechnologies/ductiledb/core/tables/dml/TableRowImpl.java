package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.api.tables.dml.TableRow;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

/**
 * This class represents a single table row.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TableRowImpl implements TableRow {

    private final Map<String, byte[]> values = new HashMap<>();

    @Override
    public byte[] getBytes(String column) {
	return values.get(column);
    }

    @Override
    public String getString(String column) {
	return Bytes.toString(values.get(column));
    }

    public void add(String column, byte[] value) {
	values.put(column, value);
    }

}
