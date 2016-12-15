package com.puresoltechnologies.ductiledb.jdbc;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.core.DuctileDB;
import com.puresoltechnologies.ductiledb.core.DuctileDBBootstrap;
import com.puresoltechnologies.ductiledb.core.DuctileDBConfiguration;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraphFactory;

public class DuctileConnection implements Connection, DuctileWrapper {

    private static final Logger logger = LoggerFactory.getLogger(DuctileConnection.class);

    private final DuctileDatabaseMetaData metaData;
    private boolean closed = false;

    private final URL url;
    private final File configFile;
    private DuctileDB ductileDB;
    private Graph graph;

    private int transactionIsolation = Connection.TRANSACTION_NONE;

    private SQLWarning warnings = null;

    private int holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

    private String catalog = "table_store";

    private String schema = "system";

    private Properties clientInfo = new Properties();

    public DuctileConnection(URL url) throws SQLException {
	this.url = url;
	configFile = new File(url.getPath());
	open();
	metaData = new DuctileDatabaseMetaData(this);
	logger.info("New connection to: '" + url + "'");
    }

    public URL getUrl() {
	return url;
    }

    public File getConfigFile() {
	return configFile;
    }

    public DuctileDB getDuctileDB() {
	return ductileDB;
    }

    private void open() throws SQLException {
	try {
	    DuctileDBConfiguration configuration = DuctileDBBootstrap.readConfiguration(getUrl());
	    DuctileDBBootstrap.start(configuration);
	    ductileDB = DuctileDBBootstrap.getInstance();
	    BaseConfiguration baseConfiguration = new BaseConfiguration();
	    graph = DuctileGraphFactory.createGraph(ductileDB.getGraph(), baseConfiguration);
	} catch (IOException e) {
	    throw new SQLException("Could not open graph.", e);
	}
    }

    @Override
    public void checkClosed() throws SQLException {
	if (isClosed()) {
	    throw new SQLException("This connection has been closed.");
	}
    }

    @Override
    public Statement createStatement() throws SQLException {
	return new DuctileStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
	return new DuctilePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
	// intentionally left empty
	return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
	// TODO Auto-generated method stub
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public void commit() throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public void rollback() throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public void close() throws SQLException {
	try {
	    graph.close();
	} catch (Exception e) {
	    throw new SQLException("Could not close graph.", e);
	} finally {
	    closed = true;
	}
    }

    @Override
    public boolean isClosed() throws SQLException {
	return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
	return metaData;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public boolean isReadOnly() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
	this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
	return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
	this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
	return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
	return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
	warnings = null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
	    throws SQLException {
	// TODO Auto-generated method stub
	return new DuctilePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
	this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
	return holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
	    int resultSetHoldability) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
	    int resultSetHoldability) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Clob createClob() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
	clientInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
	this.clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
	return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
	return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
	this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
	return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
	// TODO Auto-generated method stub
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

}
