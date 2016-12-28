package com.puresoltechnologies.ductiledb.engine.io;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * This class is used to filter MD5 sum files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Md5FilenameFilter implements FilenameFilter {

    private static final String FILE_PREFIX = LogStructuredStore.DB_FILE_PREFIX + "-";

    @Override
    public boolean accept(File dir, String name) {
	return name.startsWith(FILE_PREFIX) && name.endsWith(LogStructuredStore.MD5_FILE_SUFFIX);
    }

}
