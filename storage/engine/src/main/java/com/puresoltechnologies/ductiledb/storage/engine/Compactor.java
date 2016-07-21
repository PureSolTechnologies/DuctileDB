package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableDataEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableDataIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableWriter;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.engine.utils.DbFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.utils.StopWatch;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for compaction of column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Compactor {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File commitLogFile;
    private final int blockSize;
    private final int bufferSize;
    private final long maxDataFileSize;

    public Compactor(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File commitLogFile, int blockSize,
	    int bufferSize, long maxDataFileSize) {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.commitLogFile = commitLogFile;
	this.blockSize = blockSize;
	this.bufferSize = bufferSize;
	this.maxDataFileSize = maxDataFileSize;
    }

    public void runCompaction() throws StorageException {
	try {
	    logger.info("Start compaction for '" + commitLogFile + "'...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    performCompaction();
	    deleteCommitLogFiles();
	    stopWatch.stop();
	    logger.info("Compaction for '" + commitLogFile + "' finished in " + stopWatch.getMillis() + "ms.");
	} catch (IOException e) {
	    throw new StorageException("Could not run compaction.", e);
	}
    }

    private List<File> findDataFiles() {
	List<File> dataFiles = new ArrayList<>();
	for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new DbFilenameFilter())) {
	    dataFiles.add(file);
	}
	Collections.sort(dataFiles);
	return dataFiles;
    }

    private void performCompaction() throws StorageException, IOException {
	SSTableReader commitLogReader = new SSTableReader(storage, commitLogFile, blockSize);
	try (SSTableDataIterable commitLogData = commitLogReader.readData()) {
	    Iterator<SSTableDataEntry> commitLogIterator = commitLogData.iterator();
	    List<File> dataFiles = findDataFiles();
	    String baseFilename = ColumnFamilyEngine.createBaseFilename(ColumnFamilyEngine.DB_FILE_PREFIX);
	    integrateCommitLog(commitLogIterator, dataFiles, baseFilename);
	}
    }

    private void integrateCommitLog(Iterator<SSTableDataEntry> commitLogIterator, List<File> dataFiles,
	    String baseFilename) throws StorageException, IOException {
	SSTableDataEntry commitLogNext = commitLogIterator.next();
	int fileCount = 0;
	SSTableWriter writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
		baseFilename + "-" + fileCount, bufferSize);
	try {
	    for (File dataFile : dataFiles) {
		SSTableReader dataReader = new SSTableReader(storage, dataFile, blockSize);
		byte[] commitLogRowKey = commitLogNext.getRowKey();
		try (SSTableDataIterable data = dataReader.readData()) {
		    for (SSTableDataEntry dataEntry : data) {
			byte[] dataRowKey = dataEntry.getRowKey();
			if (comparator.compare(dataRowKey, commitLogRowKey) == 0) {
			    writer.write(dataRowKey, dataEntry.update(commitLogNext.getColumns()));
			} else if (comparator.compare(dataRowKey, commitLogRowKey) < 0) {
			    writer.write(dataRowKey, dataEntry.getColumns());
			} else {
			    while (comparator.compare(dataRowKey, commitLogRowKey) > 0) {
				writer.write(dataRowKey, commitLogNext.getColumns());
				commitLogNext = commitLogIterator.next();
				commitLogRowKey = commitLogNext.getRowKey();
			    }
			}
			if (writer.getDataFileSize() >= maxDataFileSize) {
			    writer.close();
			    fileCount++;
			    writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
				    baseFilename + "-" + fileCount, bufferSize);
			}
		    }
		}
	    }
	    if (commitLogNext != null) {
		writer.write(commitLogNext);
		while (commitLogIterator.hasNext()) {
		    SSTableDataEntry commitLogEntry = commitLogIterator.next();
		    writer.write(commitLogEntry.getRowKey(), commitLogEntry.getColumns());
		    if (writer.getDataFileSize() >= maxDataFileSize) {
			writer.close();
			fileCount++;
			writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
				baseFilename + "-" + fileCount, bufferSize);
		    }
		}
	    }
	} finally {
	    writer.close();
	}
    }

    private void deleteCommitLogFiles() {
	storage.delete(SSTableReader.getIndexName(commitLogFile));
	storage.delete(commitLogFile);
    }

}
