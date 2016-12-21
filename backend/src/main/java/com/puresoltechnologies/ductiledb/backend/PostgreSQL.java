package com.puresoltechnologies.ductiledb.backend;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs the actual connection to PostgreSQL database and handles
 * the connection pool.
 * 
 * @author Rick-Rainer Ludwig
 */
public class PostgreSQL implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQL.class);
    private static PostgreSQL postgreSQLConnection;

    public synchronized static void connect(DatabaseConfiguration configuration) throws SQLException {
	if (postgreSQLConnection == null) {
	    postgreSQLConnection = new PostgreSQL(configuration);
	}
    }

    public synchronized static void disconnect() throws IOException {
	if (postgreSQLConnection != null) {
	    try {
		postgreSQLConnection.close();
	    } finally {
		postgreSQLConnection = null;
	    }
	}
    }

    public static PostgreSQL get() {
	return postgreSQLConnection;
    }

    private final DatabaseConfiguration configuration;
    private final ConnectionPool connectionPool;

    private PostgreSQL(DatabaseConfiguration configuration) throws SQLException {
	this.configuration = configuration;
	logger.info("Initializing PostgreSQL driver v" + org.postgresql.Driver.getVersion() + ".");
	connectionPool = new ConnectionPool(configuration);

    }

    @Override
    public void close() throws IOException {
	connectionPool.close();
    }

    public Connection getConnection() throws SQLException {
	try {
	    return new PooledConnection(connectionPool, connectionPool.borrowObject());
	} catch (Exception e) {
	    throw new SQLException("Could not borrow connection from pool.", e);
	}
    }
}
