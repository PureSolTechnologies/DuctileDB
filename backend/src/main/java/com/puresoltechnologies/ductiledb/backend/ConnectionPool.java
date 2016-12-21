package com.puresoltechnologies.ductiledb.backend;

import java.sql.Connection;

import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * This class is used to keep and handle the connection pool for PostgreSQL.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ConnectionPool extends GenericObjectPool<Connection> {

    public ConnectionPool(DatabaseConfiguration configuration) {
	super(new ConnectionFactory(configuration));
	setMaxTotal(configuration.getMaxConnections());
	setMaxIdle(configuration.getMaxConnections());
	setMinIdle(configuration.getMinConnections());
    }

    
    
}
