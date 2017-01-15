package com.puresoltechnologies.ductiledb.logstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This is the central interface for a simple Log Structured Storage engine. It
 * only supports put, delete and get based on the primary index. This store can
 * be used as base implementation for a Column Family Engine and also for
 * secondary indizes.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface LogStructuredStore extends StorageOperations, AutoCloseable {

    public static final String DB_FILE_PREFIX = "DB";
    public static final String DATA_FILE_SUFFIX = ".data";
    public static final String INDEX_FILE_SUFFIX = ".index";
    public static final String DELETED_FILE_SUFFIX = ".delete";
    public static final String COMPACTED_FILE_SUFFIX = ".compacted";
    public static final String MD5_FILE_SUFFIX = ".md5";
    public static final String METADATA_SUFFIX = ".metadata";
    public static final String COMMIT_LOG_PREFIX = "CommitLog";

    public static LogStructuredStore create(Storage storage, File directory, LogStoreConfiguration configuration)
	    throws IOException {
	storage.createDirectory(directory);
	try (BufferedOutputStream parameterFile = storage.create(new File(directory, "configuration.json"))) {
	    ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	    objectMapper.writeValue(parameterFile, configuration);
	}
	LogStructuredStoreImpl store = new LogStructuredStoreImpl(storage, directory, configuration);
	store.open();
	return store;
    }

    public static LogStructuredStore reopen(Storage storage, File directory) throws IOException {
	try (BufferedInputStream parameterFile = storage.open(new File(directory, "configuration.json"))) {
	    ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	    LogStoreConfiguration configuration = objectMapper.readValue(parameterFile, LogStoreConfiguration.class);
	    LogStructuredStoreImpl store = new LogStructuredStoreImpl(storage, directory, configuration);
	    store.open();
	    return store;
	}
    }

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
	return new File(dataFile.getParent(), dataFile.getName().replace(LogStructuredStore.DATA_FILE_SUFFIX,
		LogStructuredStore.COMPACTED_FILE_SUFFIX));
    }

    @Override
    public void open();

    @Override
    public void close();

}
