package com.puresoltechnologies.ductiledb.storage.engine.io.data;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBOutputStream;

public class DataOutputStream extends DuctileDBOutputStream {

    public DataOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);
    }

    public synchronized void writeRow(RowKey rowKey, ColumnMap columns) throws IOException {
	byte[] key = rowKey.getKey();
	// Row key
	writeData(Bytes.toBytes(key.length));
	writeData(key);
	// Column number
	Set<Entry<byte[], byte[]>> entrySet = columns.entrySet();
	writeData(Bytes.toBytes(entrySet.size()));
	// Columns
	for (Entry<byte[], byte[]> column : entrySet) {
	    // Column key
	    byte[] columnKey = column.getKey();
	    writeData(Bytes.toBytes(columnKey.length));
	    writeData(columnKey);
	    // Column value
	    byte[] columnValue = column.getValue();
	    writeData(Bytes.toBytes(columnValue.length));
	    if (columnValue.length > 0) {
		writeData(columnValue);
	    }
	}
    }

}
