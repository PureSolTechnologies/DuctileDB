package com.puresoltechnologies.ductiledb.storage.engine.io.commitlog;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;

/**
 * This {@link FilenameFilter} is used to list commit logs.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogFilenameFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
	return name.startsWith(ColumnFamilyEngine.COMMIT_LOG_PREFIX)
		&& name.endsWith(ColumnFamilyEngine.DATA_FILE_SUFFIX);
    }

}
