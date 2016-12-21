package com.puresoltechnologies.ductiledb.blobstore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.hash.HashId;
import com.puresoltechnologies.commons.misc.hash.HashIdCreatorInputStream;
import com.puresoltechnologies.ductiledb.backend.DuctileDBException;
import com.puresoltechnologies.ductiledb.backend.PostgreSQL;

/**
 * This is the base implementation of the BLOB store for DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class BlobStoreImpl implements BlobStore {

    private static final Logger logger = LoggerFactory.getLogger(BlobStoreImpl.class);

    private final BlobStoreConfiguration configuration;
    private final PostgreSQL postgreSQL;
    private final PreparedStatement readStatement;
    private final PreparedStatement readChunkStatement;

    public BlobStoreImpl(BlobStoreConfiguration configuration, PostgreSQL postgreSQL) {
	super();
	this.configuration = configuration;
	this.postgreSQL = postgreSQL;
	try (Connection connection = postgreSQL.getConnection()) {

	    readStatement = connection.prepareStatement("SELECT size, chunknum FROM blobstore.blobs WHERE hashid=?");
	    readChunkStatement = connection
		    .prepareStatement("SELECT data FROM blobstore.blob_chunks WHERE hashid=? AND id=?");
	} catch (Exception e) {
	    throw new DuctileDBException("Could not prepare statements.");
	}
	initialize();
    }

    private void initialize() {
	try {
	    logger.info("Initialize Bloob service...");
	    try (Connection connection = postgreSQL.getConnection()) {
		Statement statement = connection.createStatement();
		statement.execute("CREATE SCHEMA IF NOT EXISTS blobstore");
		statement.execute("CREATE TABLE IF NOT EXISTS blobstore.blobs (" + //
			"hashid varchar(72) PRIMARY KEY, " + //
			"size bigint," + //
			"chunknum INTEGER" + //
			")");
		statement.execute("CREATE TABLE IF NOT EXISTS blobstore.blob_chunks (" + //
			"hashid varchar(72)  REFERENCES blobstore.blobs(hashid) ON DELETE CASCADE," + //
			"id INTEGER NOT NULL," + //
			"size INTEGER NOT NULL," + //
			"data BYTEA NOT NULL," + //
			"PRIMARY KEY(hashid, id)" + //
			")");
		connection.commit();
		logger.info("Bloob service initialized.");
	    }
	} catch (Exception e) {
	    throw new DuctileDBException("Could not initialize BlobStore.", e);
	}
    }

    @Override
    public long getBlobCount() throws SQLException {
	try (Connection connection = postgreSQL.getConnection();
		PreparedStatement isAvailableStatement = connection
			.prepareStatement("SELECT count(hashid) as count FROM blobstore.blobs");
		ResultSet result = isAvailableStatement.executeQuery()) {
	    if (!result.next()) {
		return 0;
	    }
	    return result.getInt("count");
	}
    }

    @Override
    public long getBlobStoreSize() throws SQLException {
	try (Connection connection = postgreSQL.getConnection();
		PreparedStatement isAvailableStatement = connection
			.prepareStatement("SELECT sum(size) as size FROM blobstore.blobs");
		ResultSet result = isAvailableStatement.executeQuery()) {
	    if (!result.next()) {
		return 0;
	    }
	    return result.getInt("size");
	}
    }

    @Override
    public boolean isBlobAvailable(HashId hashId) throws SQLException {
	if (hashId == null) {
	    throw new IllegalArgumentException("hashId must not be null.");
	}
	try (Connection connection = postgreSQL.getConnection();
		PreparedStatement isAvailableStatement = connection
			.prepareStatement("SELECT count(*) as count FROM blobstore.blobs WHERE hashid=?")) {
	    isAvailableStatement.setString(1, hashId.toString());
	    try (ResultSet result = isAvailableStatement.executeQuery()) {
		return result.getInt("count") == 1;
	    }
	}
    }

    @Override
    public long getBlobSize(HashId hashId) throws SQLException {
	if (hashId == null) {
	    throw new IllegalArgumentException("hashId must not be null.");
	}
	try (Connection connection = postgreSQL.getConnection();
		PreparedStatement getSizeStatement = connection
			.prepareStatement("SELECT size FROM blobstore.blobs WHERE hashid=?")) {
	    getSizeStatement.setString(1, hashId.toString());
	    try (ResultSet result = getSizeStatement.executeQuery()) {
		return result.getLong("size");
	    }
	}
    }

    @Override
    public InputStream readBlob(HashId hashId) {
	// if (!isBlobAvailable(hashId)) {
	// throw new FileNotFoundException("Could not find file with hash id '"
	// + hashId + "'.");
	// }
	// File path = createPath(hashId);
	// return storage.open(path);
	// TODO
	return null;
    }

    @Override
    public HashId storeBlob(InputStream inputStream) throws SQLException, IOException {
	HashIdCreatorInputStream hashIdCreatorInputStream = new HashIdCreatorInputStream(inputStream);
	File file = File.createTempFile("blobstore_storage_", ".dump");
	try {
	    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
		IOUtils.copy(hashIdCreatorInputStream, fileOutputStream);
	    }
	    HashId hashId = hashIdCreatorInputStream.getHashId();
	    try (FileInputStream fileInputStream = new FileInputStream(file)) {
		storeBlob(hashId, fileInputStream);
		return hashId;
	    }
	} finally {
	    if (!file.delete()) {
		logger.warn("Could not delete temporary file '" + file.getPath() + "'.");
	    }
	}
    }

    @Override
    public void storeBlob(HashId hashId, InputStream inputStream) throws SQLException, IOException {
	if (hashId == null) {
	    throw new IllegalArgumentException("hashId must not be null.");
	}
	if (inputStream == null) {
	    throw new IllegalArgumentException("inputStream must not be null.");
	}
	try (Connection connection = postgreSQL.getConnection()) {
	    try (PreparedStatement storeStatement = connection
		    .prepareStatement("INSERT INTO blobstore.blobs (hashid) VALUES (?)")) {
		storeStatement.setString(1, hashId.toString());
		storeStatement.execute();
	    }
	    try (PreparedStatement storeChunkStatement = connection.prepareStatement(
		    "INSERT INTO blobstore.blob_chunks (hashid, id, size, data) VALUES (?, ?, ?, ?)")) {
		storeChunkStatement.setString(1, hashId.toString());
		byte[] buffer = new byte[configuration.getChunkSize()];
		int totalSize = 0;
		int size;
		int chunkId = 0;
		do {
		    size = inputStream.read(buffer);
		    if (size > 0) {
			storeChunkStatement.setInt(2, chunkId);
			storeChunkStatement.setInt(3, size);
			storeChunkStatement.setBinaryStream(4, new ByteArrayInputStream(buffer, 0, size), size);
			storeChunkStatement.execute();
			totalSize += size;
		    }
		    chunkId++;
		} while (size == 8192);
		try (PreparedStatement storeFinalStatement = connection
			.prepareStatement("UPDATE blobstore.blobs SET size=?, chunknum=? WHERE hashid=?")) {
		    storeFinalStatement.setLong(1, totalSize);
		    storeFinalStatement.setInt(2, chunkId);
		    storeFinalStatement.setString(3, hashId.toString());
		    storeFinalStatement.execute();
		}
		connection.commit();
	    }
	}

    }

    @Override
    public boolean removeBlob(HashId hashId) throws SQLException {
	if (hashId == null) {
	    throw new IllegalArgumentException("hashId must not be null.");
	}
	try (Connection connection = postgreSQL.getConnection();
		PreparedStatement dropStatement = connection
			.prepareStatement("DROP FROM blobstore.blobs WHERE hashid=?")) {
	    dropStatement.setString(1, hashId.toString());
	    dropStatement.execute();
	    connection.commit();
	    return true;
	}
    }
}
