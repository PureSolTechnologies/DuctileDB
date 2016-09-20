package com.puresoltechnologies.ductiledb.storage.engine.lss;

import java.io.Closeable;
import java.io.File;
import java.time.Instant;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;

/**
 * This is the central interface for a simple Log Structured Storage engine. It
 * only supports put, delete and get based on the primary index. This store can
 * be used as base implementation for a Column Family Engine and also for
 * secondary indizes.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface LogStructuredStore extends Closeable {

    public static final String DB_FILE_PREFIX = "DB";
    public static final String DATA_FILE_SUFFIX = ".data";
    public static final String INDEX_FILE_SUFFIX = ".index";
    public static final String DELETED_FILE_SUFFIX = ".delete";
    public static final String COMPACTED_FILE_SUFFIX = ".compacted";
    public static final String MD5_FILE_SUFFIX = ".md5";
    public static final String METADATA_SUFFIX = ".metadata";
    public static final String COMMIT_LOG_PREFIX = "CommitLog";

    public static String createFilename(String filePrefix, Instant timestamp, int number, String suffix) {
	StringBuilder buffer = new StringBuilder(filePrefix);
	buffer.append('-');
	buffer.append(timestamp.getEpochSecond());
	int millis = timestamp.getNano() / 1000000;
	if (millis < 100) {
	    if (millis < 10) {
		buffer.append("00");
	    } else {
		buffer.append('0');
	    }
	}
	buffer.append(millis);
	buffer.append("-");
	buffer.append(number);
	buffer.append(suffix);
	return buffer.toString();
    }

    public static String createBaseFilename(String filePrefix) {
	Instant timestamp = Instant.now();
	StringBuilder buffer = new StringBuilder(filePrefix);
	buffer.append('-');
	buffer.append(timestamp.getEpochSecond());
	int millis = timestamp.getNano() / 1000000;
	if (millis < 100) {
	    if (millis < 10) {
		buffer.append("00");
	    } else {
		buffer.append('0');
	    }
	}
	buffer.append(millis);
	return buffer.toString();
    }

    public static String createFilename(String baseFilename, int number, String suffix) {
	StringBuilder buffer = new StringBuilder(baseFilename);
	buffer.append("-");
	buffer.append(number);
	buffer.append(suffix);
	return buffer.toString();
    }

    public static File getCompactedName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.COMPACTED_FILE_SUFFIX));
    }

    /**
     * This method is used to put additional columns to the given row.
     * 
     * @param rowKey
     * @param columnValues
     */
    public void put(byte[] rowKey, ColumnMap columnValues);

    /**
     * This metho retrieves the columns from the given row.
     * 
     * @param rowKey
     * @return
     */
    public ColumnMap get(byte[] rowKey);

    /**
     * This method returns a scanner for the column family.
     * 
     * @return
     */
    public ColumnFamilyScanner getScanner(byte[] startRowKey, byte[] endRowKey);

    /**
     * This method removes the given row.
     * 
     * @param rowKey
     */
    public void delete(byte[] rowKey);

    public void delete(byte[] rowKey, Set<byte[]> columns);

}
