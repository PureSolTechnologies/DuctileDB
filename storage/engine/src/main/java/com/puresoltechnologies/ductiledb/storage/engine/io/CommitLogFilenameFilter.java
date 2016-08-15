package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This {@link FilenameFilter} is used to list commit logs.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogFilenameFilter implements FilenameFilter {

    private final Storage storage;

    public CommitLogFilenameFilter(Storage storage) {
	this.storage = storage;
    }

    @Override
    public boolean accept(File dir, String name) {
	if (name.startsWith(ColumnFamilyEngine.COMMIT_LOG_PREFIX)
		&& name.endsWith(ColumnFamilyEngine.DATA_FILE_SUFFIX)) {
	    File compactedName = ColumnFamilyEngineImpl.getCompactedName(new File(dir, name));
	    boolean exists = storage.exists(compactedName);
	    return !exists;
	}
	return false;
    }

}
