package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngine;

/**
 * This class is used to filter meta data files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class MetadataFilenameFilter implements FilenameFilter {

    private static final String FILE_PREFIX = ColumnFamilyEngine.DB_FILE_PREFIX + "-";

    @Override
    public boolean accept(File dir, String name) {
	return name.startsWith(FILE_PREFIX) && name.endsWith(ColumnFamilyEngine.METADATA_SUFFIX);
    }

}
