package com.puresoltechnologies.ductiledb.logstore.io.filter;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * This class is used to filter meta data files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class MetadataFilenameFilter implements FilenameFilter {

    private static final String FILE_PREFIX = LogStructuredStore.DB_FILE_PREFIX + "-";

    @Override
    public boolean accept(File dir, String name) {
	return name.startsWith(FILE_PREFIX) && name.endsWith(LogStructuredStore.METADATA_SUFFIX);
    }

}
