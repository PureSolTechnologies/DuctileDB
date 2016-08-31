package com.puresoltechnologies.ductiledb.storage.engine.cf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.lss.LogStructuredStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamilyEngineImpl extends LogStructuredStoreImpl implements ColumnFamilyEngine {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyEngineImpl.class);

    private final ColumnFamilyDescriptor columnFamilyDescriptor;

    public ColumnFamilyEngineImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	super(storage, //
		columnFamilyDescriptor.getDirectory(), //
		configuration.getMaxCommitLogSize(), //
		configuration.getMaxDataFileSize(), //
		configuration.getBufferSize(), //
		configuration.getMaxFileGenerations());
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	logger.info("Starting column family engine '" + toString() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	open();
	stopWatch.stop();
	logger.info("Column family engine '" + toString() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public final byte[] getName() {
	return columnFamilyDescriptor.getName();
    }

    @Override
    public final ColumnFamilyDescriptor getDescriptor() {
	return columnFamilyDescriptor;
    }

    @Override
    public String toString() {
	TableDescriptor table = columnFamilyDescriptor.getTable();
	return "engine:" + table.getNamespace().getName() + "." + table.getName() + "/"
		+ Bytes.toHumanReadableString(columnFamilyDescriptor.getName());
    }

    @Override
    public long incrementColumnValue(byte[] rowKey, byte[] column, long incrementValue) throws StorageException {
	return incrementColumnValue(rowKey, column, 1l, incrementValue);
    }

    @Override
    public long incrementColumnValue(byte[] rowKey, byte[] column, long startValue, long incrementValue)
	    throws StorageException {
	long result = startValue;
	getWriteLock().lock();
	try {
	    ColumnMap columnMap = get(rowKey);
	    if (columnMap != null) {
		ColumnValue oldValueBytes = columnMap.get(column);
		if (oldValueBytes != null) {
		    long oldValue = Bytes.toLong(oldValueBytes.getValue());
		    result = oldValue + incrementValue;
		}
	    } else {
		columnMap = new ColumnMap();
	    }
	    columnMap.put(column, new ColumnValue(Bytes.toBytes(result), null));
	    writeCommitLog(new Key(rowKey), null, columnMap);
	} finally {
	    getWriteLock().unlock();
	}
	return result;
    }

    @Override
    public void createIndex(SecondaryIndexDescriptor indexDescriptor) {
	// TODO Auto-generated method stub

    }

    @Override
    public void dropIndex(String name) {
	// TODO Auto-generated method stub

    }

    @Override
    public SecondaryIndexDescriptor getIndex(String name) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterable<SecondaryIndexDescriptor> getIndizes() {
	// TODO Auto-generated method stub
	return null;
    }
}