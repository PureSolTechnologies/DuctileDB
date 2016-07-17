package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnEntry;
import com.puresoltechnologies.ductiledb.storage.engine.LogEntry;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

/**
 * This class is used to write to a commit log provided via OutputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogWriter implements Closeable {

    private final OutputStream outputStream;

    public CommitLogWriter(OutputStream outputStream) {
	super();
	this.outputStream = outputStream;
    }

    @Override
    public void close() throws IOException {
	outputStream.close();
    }

    public void write(LogEntry entry) throws IOException {
	byte[] timestamp = Bytes.toBytes(entry.getTimestamp());
	byte[] rowKey = entry.getRowKey();
	outputStream.write(rowKey.length);
	outputStream.write(rowKey);
	outputStream.write(timestamp);

	for (ColumnEntry columnEntry : entry.getColumns()) {
	    byte[] key = columnEntry.getKey();
	    byte[] value = columnEntry.getValue();
	    outputStream.write(key.length);
	    outputStream.write(key);
	    outputStream.write(value.length);
	    outputStream.write(value);
	}
    }
}
