package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;

/**
 * This class is used to filter database files. The database files are compacted
 * already with sorted entries and indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DataFilenameFilter implements FilenameFilter {

    private static final String FILE_PREFIX = ColumnFamilyEngine.DB_FILE_PREFIX + "-";

    private final String timestamp;

    public DataFilenameFilter() {
	this.timestamp = null;
    }

    public DataFilenameFilter(String timestamp) {
	this.timestamp = timestamp;
    }

    @Override
    public boolean accept(File dir, String name) {
	if (timestamp == null) {
	    return name.startsWith(FILE_PREFIX) && name.endsWith(ColumnFamilyEngine.DATA_FILE_SUFFIX);
	}
	return name.equals(FILE_PREFIX + timestamp + ColumnFamilyEngine.DATA_FILE_SUFFIX);
    }
}
