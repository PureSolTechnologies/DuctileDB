package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.StopWatch;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamilyEngine} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TableEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TableEngine.class);

    private final Storage storage;
    private final TableDescriptor tableDescriptor;
    private final DatabaseEngineConfiguration configuration;
    private final Map<String, ColumnFamilyEngine> columnFamilyEngines = new HashMap<>();

    public TableEngine(Storage storage, TableDescriptor tableDescriptor, DatabaseEngineConfiguration configuration)
	    throws StorageException {
	super();
	this.storage = storage;
	this.tableDescriptor = tableDescriptor;
	this.configuration = configuration;
	logger.info("Starting table engine '" + tableDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	initializeColumnFamilyEngines();
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void initializeColumnFamilyEngines() throws StorageException {
	for (ColumnFamilyDescriptor columnFamilyDescriptor : tableDescriptor.getColumnFamilies()) {
	    columnFamilyEngines.put(columnFamilyDescriptor.getName(),
		    new ColumnFamilyEngine(storage, columnFamilyDescriptor, configuration));
	}
    }

    @Override
    public void close() throws IOException {
	logger.info("Closing table engine '" + tableDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (ColumnFamilyEngine columnFamilyEngine : columnFamilyEngines.values()) {
	    columnFamilyEngine.close();
	}
	storage.close();
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' closed in " + stopWatch.getMillis() + "ms.");
    }
}
