package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.io.File;
import java.io.FilenameFilter;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;

/**
 * This class is used to filter database files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Md5FilenameFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
	return name.startsWith(ColumnFamilyEngine.DB_FILE_PREFIX) && name.endsWith(ColumnFamilyEngine.MD5_FILE_SUFFIX);
    }

}
