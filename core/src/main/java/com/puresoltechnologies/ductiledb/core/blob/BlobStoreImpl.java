package com.puresoltechnologies.ductiledb.core.blob;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.puresoltechnologies.commons.misc.hash.HashId;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactoryServiceException;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This is the base implementation of the BLOB store for DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class BlobStoreImpl implements BlobStore {

    /**
     * Root directory for DuctileDB's BLOB store.
     */
    private static final File filePath = new File("blobs");

    private static final Logger logger = LoggerFactory.getLogger(BlobStoreImpl.class);
    private final Storage storage;

    public BlobStoreImpl(Map<String, String> configuration) throws StorageFactoryServiceException {
	this.storage = StorageFactory.getStorageInstance(configuration);
    }

    /**
     * The constructor using a {@link FileSystem} to reference Hadoop.
     * 
     * @param fileSystem
     *            is the {@link FileSystem} of Hadoop to store objects to.
     */
    public BlobStoreImpl(Storage storage) {
	super();
	this.storage = storage;
	initialize();
    }

    private void initialize() {
	logger.info("Initialize Bloob service...");
	if (!storage.exists(filePath)) {
	    throw new RuntimeException("Could not initialize Bloob Service due to missing file directory in HDFS.");
	}
	FileStatus fileStatus = storage.getFileStatus(filePath);
	if (!fileStatus.isDirectory()) {
	    throw new RuntimeException(
		    "Could not initialize Bloob Service due to missing file directory in HDFS. Path exists, but it is not a directory.");
	}
	logger.info("Bloob service initialized.");
    }

    private File createPath(HashId hashId) {
	String hash = hashId.getHash();
	StringBuffer child = new StringBuffer();
	child.append(hash.substring(0, 2));
	child.append("/");
	child.append(hash.substring(2, 4));
	child.append("/");
	child.append(hash.substring(4, 6));
	child.append("/");
	child.append(hash.substring(6, 8));
	child.append("/");
	child.append(hash.substring(8));
	return new File(filePath, child.toString());
    }

    @Override
    public boolean isBlobAvailable(HashId hashId) throws IOException {
	File path = createPath(hashId);
	return storage.exists(path);
    }

    @Override
    public long getBlobSize(HashId hashId) throws FileNotFoundException, IOException {
	File path = createPath(hashId);
	Iterator<File> files = storage.list(path);
	if (!files.hasNext()) {
	    throw new FileNotFoundException("Could not find file with hash id '" + hashId + "'.");
	}
	return storage.getFileStatus(files.next()).getLength();
    }

    @Override
    public InputStream readBlob(HashId hashId) throws FileNotFoundException, IOException {
	if (!isBlobAvailable(hashId)) {
	    throw new FileNotFoundException("Could not find file with hash id '" + hashId + "'.");
	}
	File path = createPath(hashId);
	return storage.open(path);
    }

    @Override
    public void storeBlob(HashId hashId, InputStream inputStream) throws IOException {
	File path = createPath(hashId);
	try (OutputStream outputStream = storage.create(path);) {
	    ByteStreams.copy(inputStream, outputStream);
	}
    }

    @Override
    public boolean removeBlob(HashId hashId) throws IOException {
	if (!isBlobAvailable(hashId)) {
	    return false;
	}
	File path = createPath(hashId);
	if (!storage.delete(path)) {
	    throw new IOException("Could not delete file for hash id '" + hashId + "'.");
	}
	return true;
    }

    @Override
    public void close() throws IOException {
	storage.close();
    };
}
