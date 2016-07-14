package com.puresoltechnologies.ductiledb.storage.engine.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This method is used to read SSTable files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableFileReader implements Closeable {

    private final File file;
    private final InputStream inputStream;

    public SSTableFileReader(File file) throws IOException, SSTableException {
	super();
	this.file = file;
	this.inputStream = new FileInputStream(file);
	readHeader();
    }

    private void readHeader() throws IOException, SSTableException {
	byte[] identifier = new byte[SSTableHeader.SSTABLE_IDENTIFIER.length()];
	String identifierString = new String(identifier);
	int size = inputStream.read(identifier);
	if ((size != SSTableHeader.SSTABLE_IDENTIFIER.length())
		|| (!SSTableHeader.SSTABLE_IDENTIFIER.equals(identifierString))) {
	    throw new SSTableException("Illegal SSTable. The identifier '" + identifierString + "' is wrong.");
	}
    }

    public final File getFile() {
	return file;
    }

    @Override
    public void close() throws IOException {
	inputStream.close();
    }
}
