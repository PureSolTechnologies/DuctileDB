package com.puresoltechnologies.ductiledb.storage.engine.io.data;

import java.io.BufferedInputStream;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;

public class DataInputStream extends DuctileDBInputStream {

    public DataInputStream(BufferedInputStream bufferedOutputStream) {
	super(bufferedOutputStream);
    }

    public ColumnFamilyRow readRow() throws IOException {
	byte[] buffer = new byte[4];
	int len = read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	int length = Bytes.toInt(buffer);
	byte[] rowKey = new byte[length];
	len = read(rowKey);
	if (len < length) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	len = read(buffer, 0, 4);
	if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	int columnCount = Bytes.toInt(buffer);
	ColumnMap columns = new ColumnMap();
	for (int i = 0; i < columnCount; ++i) {
	    // Column key...
	    len = read(buffer, 0, 4);
	    if (len < 4) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    length = Bytes.toInt(buffer);
	    byte[] columnKey = new byte[length];
	    len = read(columnKey);
	    if (len < length) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    // Column value...
	    len = read(buffer, 0, 4);
	    if (len < 4) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    length = Bytes.toInt(buffer);
	    byte[] columnValue = new byte[length];
	    len = read(columnValue);
	    if (len < length) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    columns.put(columnKey, columnValue);
	}
	return new ColumnFamilyRow(new RowKey(rowKey), columns);
    }

}
