package com.puresoltechnologies.ductiledb.logstore.io.filter;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * This {@link FilenameFilter} is used to list commit logs.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogFilenameFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
	return name.startsWith(LogStructuredStore.COMMIT_LOG_PREFIX)
		&& name.endsWith(LogStructuredStore.DATA_FILE_SUFFIX);
    }

}
