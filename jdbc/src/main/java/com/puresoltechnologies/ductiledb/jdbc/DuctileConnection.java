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

import com.puresoltechnologies.ductiledb.core.DuctileDB;
import com.puresoltechnologies.ductiledb.core.DuctileDBBootstrap;
import com.puresoltechnologies.ductiledb.core.DuctileDBConfiguration;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraphFactory;

public class DuctileConnection implements Connection, DuctileWrapper {

    private DuctileDatabaseMetaData metaData = null;
    private boolean closed = false;

    private final URL url;
    private final File configFile;
    private DuctileDB ductileDB;
    private final Graph graph;

    public DuctileConnection(URL url) throws SQLException {
	this.url = url;
	configFile = new File(url.getPath());
	this.graph = open();
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

    private DuctileGraph open() throws SQLException {
	try {
	    DuctileDBConfiguration configuration = DuctileDBBootstrap.readConfiguration(getUrl());
	    DuctileDBBootstrap.start(configuration);
	    DuctileDB ductileDB = DuctileDBBootstrap.getInstance();
	    BaseConfiguration baseConfiguration = new BaseConfiguration();
	    return DuctileGraphFactory.createGraph(ductileDB.getGraph(), baseConfiguration);
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
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
	// TODO Auto-generated method stub
	return null;
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
	if (metaData == null) {
	    metaData = new DuctileDatabaseMetaData(this);
	}
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
	// TODO Auto-generated method stub

    }

    @Override
    public String getCatalog() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
	// TODO Auto-generated method stub

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
	// TODO Auto-generated method stub

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
	return null;
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
	// TODO Auto-generated method stub

    }

    @Override
    public int getHoldability() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
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
	// TODO Auto-generated method stub

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
	// TODO Auto-generated method stub

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
	// TODO Auto-generated method stub
	return null;
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
	// TODO Auto-generated method stub

    }

    @Override
    public String getSchema() throws SQLException {
	// TODO Auto-generated method stub
	return null;
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
