package com.puresoltechnologies.ductiledb.logstore.data;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DuctileDBOutputStream;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageOutputStream;

public class DataFileWriter implements Closeable {

    private final Storage storage;
    private final File dataFile;
    private final DuctileDBOutputStream outputStream;

    public DataFileWriter(Storage storage, File dataFile) throws IOException {
	this.storage = storage;
	this.dataFile = dataFile;
	outputStream = new DuctileDBOutputStream(storage.create(dataFile));
    }

    public DataFileWriter(StorageOutputStream storageOutputStream) throws IOException {
	this.storage = null;
	this.dataFile = null;
	outputStream = new DuctileDBOutputStream(storageOutputStream);
    }

    public synchronized void writeRow(Key rowKey, Instant tombstone, byte[] data) throws IOException {
	byte[] key = rowKey.getBytes();
	// Row key
	outputStream.writeData(Bytes.fromInt(key.length));
	outputStream.writeData(key);
	// Row tombstone
	writeTombstone(tombstone);
	// Column number
	outputStream.writeData(Bytes.fromInt(data.length));
	outputStream.writeData(data);
    }

    public void writeTombstone(Instant tombstone) throws IOException {
	if (tombstone == null) {
	    outputStream.writeData(new byte[12]);
	} else {
	    outputStream.write(tombstone);
	}
    }

    @Override
    public void close() throws IOException {
	outputStream.close();
    }

    public MessageDigest getMessageDigest() {
	return outputStream.getMessageDigest();
    }

    public long getPosition() {
	return outputStream.getPosition();
    }

    public void flush() throws IOException {
	outputStream.flush();
    }

    public void writeData(byte[] data) throws IOException {
	outputStream.write(data);
    }

}
