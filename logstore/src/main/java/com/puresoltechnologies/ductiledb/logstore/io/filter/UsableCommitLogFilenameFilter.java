package com.puresoltechnologies.ductiledb.logstore.io.filter;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This {@link FilenameFilter} is used to list commit logs.
 * 
 * @author Rick-Rainer Ludwig
 */
public class UsableCommitLogFilenameFilter implements FilenameFilter {

    private final Storage storage;

    public UsableCommitLogFilenameFilter(Storage storage) {
	this.storage = storage;
    }

    @Override
    public boolean accept(File dir, String name) {
	if (name.startsWith(LogStructuredStore.COMMIT_LOG_PREFIX)
		&& name.endsWith(LogStructuredStore.DATA_FILE_SUFFIX)) {
	    File dataFile = new File(dir, name);
	    File compactedName = LogStructuredStore.getCompactedName(dataFile);
	    if (storage.exists(compactedName)) {
		return false;
	    }
	    File indexFile = DataFileSet.getIndexName(dataFile);
	    return storage.exists(indexFile);
	}
	return false;
    }

}
