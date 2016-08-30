package com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;

public class IndexInputStream extends DuctileDBInputStream {

    private final File dataFile;

    public IndexInputStream(BufferedInputStream bufferedOutputStream) throws IOException {
	super(bufferedOutputStream);
	this.dataFile = new File(readDataFile());
    }

    private String readDataFile() throws IOException {
	byte[] buffer = new byte[4];
	int len = read(buffer, 0, 4);
	if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	int fileNameLength = Bytes.toInt(buffer);
	byte[] fileName = new byte[fileNameLength];
	len = read(fileName);
	if (len < fileNameLength) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	return Bytes.toString(fileName);
    }

    public File getIndexFile() {
	return dataFile;
    }

    public IndexEntry readEntry() throws IOException {
	byte[] buffer = new byte[8];
	int len = read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	int keyLength = Bytes.toInt(buffer);
	byte[] rowKey = new byte[keyLength];
	len = read(rowKey);
	if (len < keyLength) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	len = read(buffer, 0, 8);
	if (len < 8) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	long offset = Bytes.toLong(buffer);
	return new IndexEntry(new Key(rowKey), dataFile, offset);
    }

}
