package com.puresoltechnologies.ductiledb.jdbc;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.versioning.Version;

/**
 * This is the JDBC driver for DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDriver implements Driver {

    private static final Logger logger = Logger.getLogger(DuctileDriver.class.getName());

    static {
	try {
	    // Register the Driver with DriverManager
	    DuctileDriver driverInstance = new DuctileDriver();
	    DriverManager.registerDriver(driverInstance);
	} catch (SQLException e) {
	    throw new RuntimeException("Could not register " + DuctileDriver.class.getName() + " to "
		    + DriverManager.class.getName() + ".");
	}
    }

    static final String JDBC_DUCTILE_URL_PREFIX = "jdbc:ductile:";

    private final Version version = Version.valueOf(BuildInformation.getVersion());

    @Override
    public Connection connect(String urlString, Properties info) throws SQLException {
	if (!urlString.startsWith(JDBC_DUCTILE_URL_PREFIX)) {
	    return null;
	}
	URL url = convertToURL(urlString);
	return new DuctileConnection(url);
    }

    @Override
    public boolean acceptsURL(String urlString) throws SQLException {
	if (urlString.startsWith(JDBC_DUCTILE_URL_PREFIX)) {
	    return true;
	} else {
	    return false;
	}
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String urlString, Properties info) throws SQLException {
	if (!urlString.startsWith(JDBC_DUCTILE_URL_PREFIX)) {
	    return null;
	}
	DriverPropertyInfo[] propertyInfo = new DriverPropertyInfo[0];
	return propertyInfo;
    }

    @Override
    public int getMajorVersion() {
	return version.getMajor();
    }

    @Override
    public int getMinorVersion() {
	return version.getMinor();
    }

    @Override
    public boolean jdbcCompliant() {
	/*
	 * This is not a fully compliant JDBC driver, because we do not do SQL
	 * here...
	 */
	return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	return logger;
    }

    URL convertToURL(String urlString) throws SQLException {
	try {
	    if (!urlString.startsWith(JDBC_DUCTILE_URL_PREFIX)) {
		throw new SQLException("URL needs to start with '" + JDBC_DUCTILE_URL_PREFIX + "'.");
	    }
	    URL url = new URL(urlString.substring(JDBC_DUCTILE_URL_PREFIX.length()));
	    if (!"file".equals(url.getProtocol())) {
		throw new SQLException("URL '" + urlString
			+ "' needs to be of type 'file:' and to point to a valid 'hbase.xml' file.");
	    }
	    File file = new File(url.getPath());
	    if (!file.exists()) {
		throw new SQLException("URL '" + urlString
			+ "' needs to be of type 'file:' and to point to a valid 'hbase.xml' file.");
	    }
	    return url;
	} catch (MalformedURLException e) {
	    throw new SQLException("URL '" + urlString + "' is invalid.", e);
	}
    }
}
