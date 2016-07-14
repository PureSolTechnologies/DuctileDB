package com.puresoltechnologies.ductiledb.storage.engine.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SSTableFileWriter implements Closeable {

    private final File file;
    private final OutputStream outputStream;

    public SSTableFileWriter(File file) throws IOException {
	super();
	this.file = file;
	if (!file.exists()) {
	    this.outputStream = new FileOutputStream(file);
	    writeHeader();
	} else {
	    this.outputStream = new FileOutputStream(file);
	}
    }

    public final File getFile() {
	return file;
    }

    private void writeHeader() throws IOException {
	outputStream.write(SSTableHeader.SSTABLE_IDENTIFIER.getBytes());
    }

    @Override
    public void close() throws IOException {
	outputStream.close();
    }
}
