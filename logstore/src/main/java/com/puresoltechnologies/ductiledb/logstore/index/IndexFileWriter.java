package com.puresoltechnologies.ductiledb.logstore.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DuctileDBOutputStream;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexFileWriter implements Closeable {

    private static void writeDataFilePath(DuctileDBOutputStream outputStream, File dataFile) throws IOException {
	String path = dataFile.getPath();
	outputStream.writeData(Bytes.fromInt(path.length()));
	outputStream.writeData(Bytes.fromString(path));
    }

    public static void writeIndexEntry(DuctileDBOutputStream outputStream, Key rowKey, long offset) throws IOException {
	outputStream.writeData(Bytes.fromInt(rowKey.getBytes().length));
	outputStream.writeData(rowKey.getBytes());
	outputStream.writeData(Bytes.fromLong(offset));
    }

    private final DuctileDBOutputStream outputStream;

    public IndexFileWriter(Storage storage, File indexFile, File dataFile) throws IOException {
	super();
	this.outputStream = new DuctileDBOutputStream(storage.create(indexFile));
	writeDataFilePath(dataFile);
    }

    private synchronized void writeDataFilePath(File dataFile) throws IOException {
	writeDataFilePath(outputStream, dataFile);
    }

    public synchronized void writeIndexEntry(Key rowKey, long offset) throws IOException {
	writeIndexEntry(outputStream, rowKey, offset);
    }

    public MessageDigest getMessageDigest() {
	return outputStream.getMessageDigest();
    }

    public void flush() throws IOException {
	outputStream.flush();
    }

    @Override
    public void close() throws IOException {
	outputStream.close();
    }
}
