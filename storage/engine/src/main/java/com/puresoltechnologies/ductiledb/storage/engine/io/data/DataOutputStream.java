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

    public void writeRow(RowKey rowKey, ColumnMap columns) throws IOException {
	writeData(Bytes.toBytes(rowKey.getKey().length));
	writeData(rowKey.getKey());
	Set<Entry<byte[], byte[]>> entrySet = columns.entrySet();
	writeData(Bytes.toBytes(entrySet.size()));
	for (Entry<byte[], byte[]> column : entrySet) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    writeData(Bytes.toBytes(key.length));
	    writeData(key);
	    writeData(Bytes.toBytes(value.length));
	    writeData(value);
	}
    }

}
