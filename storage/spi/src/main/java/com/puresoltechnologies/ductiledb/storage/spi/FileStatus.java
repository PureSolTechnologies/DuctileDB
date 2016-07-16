package com.puresoltechnologies.ductiledb.storage.spi;

import java.io.File;

/**
 * This class is an abstraction to a file or directoy to provide the status of
 * it.
 * 
 * @author Rick-Rainer Ludwig
 */
public class FileStatus {

    private final File file;
    private final FileType type;
    private final boolean hidden;
    private final long length;

    public FileStatus(File file, FileType type, boolean hidden, long length) {
	this.file = file;
	this.type = type;
	this.hidden = hidden;
	this.length = length;
    }

    public File getFile() {
	return file;
    }

    public boolean isDirectory() {
	return type == FileType.DIRECTORY;
    }

    public boolean isFile() {
	return type == FileType.FILE;
    }

    public boolean isHidden() {
	return hidden;
    }

    public long getLength() {
	return length;
    }
}
