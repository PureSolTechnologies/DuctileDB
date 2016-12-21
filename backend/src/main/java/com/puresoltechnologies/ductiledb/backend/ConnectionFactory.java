package com.puresoltechnologies.ductiledb.backend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the connection factory for the connection pool.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ConnectionFactory implements PooledObjectFactory<Connection> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactory.class);

    private final DatabaseConfiguration configuration;

    public ConnectionFactory(DatabaseConfiguration configuration) {
	super();
	this.configuration = configuration;
    }

    @Override
    public PooledObject<Connection> makeObject() throws Exception {
	logger.info("Creating new connection...");
	Connection connection = DriverManager.getConnection(configuration.getJdbcUrl());
	connection.setAutoCommit(false);
	DefaultPooledObject<Connection> pooledObject = new DefaultPooledObject<>(connection);
	logger.info("Connection created.");
	return pooledObject;
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
	logger.info("Destroying old connection...");
	p.getObject().close();
	logger.info("Connection destroyed.");
    }

    @Override
    public boolean validateObject(PooledObject<Connection> p) {
	try {
	    return !p.getObject().isClosed();
	} catch (SQLException e) {
	    logger.warn("Could not validate connection.", e);
	    return false;
	}
    }

    @Override
    public void activateObject(PooledObject<Connection> p) throws Exception {
	// intentionally left blank
    }

    @Override
    public void passivateObject(PooledObject<Connection> p) throws Exception {
	// intentionally left blank
    }
}
