package com.puresoltechnologies.ductiledb.core.blob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.hash.HashId;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;

/**
 * This is the base implementation of the BLOB store for DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class BlobStoreImpl implements BlobStore {

    /**
     * Root directory for DuctileDB's BLOB store.
     */
    private static final Path filePath = new Path("/apps/DuctileDB/blobs");

    private static final Logger logger = LoggerFactory.getLogger(BlobStoreImpl.class);
    private final FileSystem fileSystem;

    public BlobStoreImpl(Configuration configuration) throws IOException {
	this.fileSystem = FileSystem.newInstance(configuration);
    }

    /**
     * The constructor using a {@link FileSystem} to reference Hadoop.
     * 
     * @param fileSystem
     *            is the {@link FileSystem} of Hadoop to store objects to.
     */
    public BlobStoreImpl(FileSystem fileSystem) {
	super();
	this.fileSystem = fileSystem;
	initialize();
    }

    private void initialize() {
	try {
	    logger.info("Initialize Bloob service...");
	    if (!fileSystem.exists(filePath)) {
		throw new RuntimeException("Could not initialize Bloob Service due to missing file directory in HDFS.");
	    }
	    FileStatus fileStatus = fileSystem.getFileStatus(filePath);
	    if (!fileStatus.isDirectory()) {
		throw new RuntimeException(
			"Could not initialize Bloob Service due to missing file directory in HDFS. Path exists, but it is not a directory.");
	    }
	    logger.info("Bloob service initialized.");
	} catch (IOException e) {
	    throw new RuntimeException("Could not initialize Bloob service.", e);
	}
    }

    private Path createPath(HashId hashId) {
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
	return new Path(filePath, child.toString());
    }

    @Override
    public boolean isBlobAvailable(HashId hashId) throws IOException {
	Path path = createPath(hashId);
	return fileSystem.exists(path);
    }

    @Override
    public long getBlobSize(HashId hashId) throws FileNotFoundException, IOException {
	Path path = createPath(hashId);
	RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, false);
	if (!files.hasNext()) {
	    throw new FileNotFoundException("Could not find file with hash id '" + hashId + "'.");
	}
	return files.next().getLen();
    }

    @Override
    public InputStream readBlob(HashId hashId) throws FileNotFoundException, IOException {
	if (!isBlobAvailable(hashId)) {
	    throw new FileNotFoundException("Could not find file with hash id '" + hashId + "'.");
	}
	Path path = createPath(hashId);
	return fileSystem.open(path);
    }

    @Override
    public void storeBlob(HashId hashId, InputStream inputStream) throws IOException {
	Path path = createPath(hashId);
	try (FSDataOutputStream outputStream = fileSystem.create(path, false);) {
	    IOUtils.copy(inputStream, outputStream);
	}
    }

    @Override
    public boolean removeBlob(HashId hashId) throws IOException {
	if (!isBlobAvailable(hashId)) {
	    return false;
	}
	Path path = createPath(hashId);
	if (!fileSystem.delete(path, false)) {
	    throw new IOException("Could not delete file for hash id '" + hashId + "'.");
	}
	return true;
    }

    @Override
    public void close() throws IOException {
	fileSystem.close();
    };
}
