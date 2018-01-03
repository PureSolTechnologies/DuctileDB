package com.puresoltechnologies.ductiledb.logstore.data;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.Row;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageInputStream;
import com.puresoltechnologies.streaming.StreamIterator;
import com.puresoltechnologies.streaming.streams.InputStreamIterator;
import com.puresoltechnologies.streaming.streams.MultiStreamSeekableInputStream;

public class DataFileReader implements CloseableIterable<Row> {

    private static Row readRow(InputStream inputStream) throws IOException {
	byte[] buffer = new byte[12];
	// Read row key
	int len = inputStream.read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	int length = Bytes.toInt(buffer);
	byte[] rowKeyBytes = new byte[length];
	len = inputStream.read(rowKeyBytes);
	if (len < length) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	Key rowKey = Key.of(rowKeyBytes);
	// Read tombstone
	len = inputStream.read(buffer, 0, 12);
	if (len < 12) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	Instant tombstone = Bytes.toTombstone(buffer);
	// Read value
	len = inputStream.read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	length = Bytes.toInt(buffer);
	byte[] rowDataBytes = new byte[length];
	len = inputStream.read(rowDataBytes);
	if (len < length) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	return new Row(rowKey, tombstone, rowDataBytes);
    }

    private static final Logger logger = LoggerFactory.getLogger(DataFileReader.class);
    private final Storage storage;
    private final File dataFile;
    private final MultiStreamSeekableInputStream<StorageInputStream> inputStream;

    public DataFileReader(Storage storage, File dataFile) throws IOException {
	this.storage = storage;
	this.dataFile = dataFile;
	inputStream = new MultiStreamSeekableInputStream<>(10, () -> storage.open(dataFile));
    }

    public long getPosition() {
	return inputStream.getPosition();
    }

    public void seek(long offset) throws IOException {
	inputStream.seek(offset);
    }

    public Row readRow() throws IOException {
	return readRow(inputStream);
    }

    public Row readRow(IndexEntry indexEntry) throws IOException {
	return readRow(indexEntry.getOffset());
    }

    public Row readRow(long offset) throws IOException {
	inputStream.seek(offset);
	return readRow();
    }

    public Row readRow(Key rowKey) throws IOException {
	Row row = readRow();
	while (row != null) {
	    if (row.getKey().equals(rowKey)) {
		return row;
	    }
	    row = readRow();
	}
	return null;
    }

    public Row readRow(Key rowKey, long startOffset, long endOffset) throws IOException {
	inputStream.seek(startOffset);
	Row row = readRow();
	while ((inputStream.getPosition() <= endOffset) && (row != null)) {
	    if (row.getKey().equals(rowKey)) {
		return row;
	    }
	    row = readRow();
	}
	return null;
    }

    @Override
    public StreamIterator<Row> iterator() {
	return new InputStreamIterator<>(inputStream, i -> {
	    try {
		return readRow(i);
	    } catch (IOException e) {
		logger.error("Could not read column family row.", e);
		return null;
	    }
	});
    }

    @Override
    public void close() throws IOException {
	inputStream.close();
    }
}
