package com.puresoltechnologies.ductiledb.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;

@Ignore
public class DuctileDriverTest {

    @BeforeClass
    public static void initialization() throws ClassNotFoundException {
	Class.forName(DuctileDriver.class.getName());
    }

    @Test
    public void testDriverManagerRegistration() {
	boolean foundDriver = false;

	Enumeration<Driver> drivers = DriverManager.getDrivers();
	while (drivers.hasMoreElements()) {
	    Driver driver = drivers.nextElement();
	    if (driver.getClass().equals(DuctileDriver.class)) {
		foundDriver = true;
	    }
	}
	assertTrue("Driver '" + DuctileDriver.class.getName() + "' was not registered.", foundDriver);
    }

    @Test
    public void testGetDriver() throws SQLException {
	Driver driver = DriverManager.getDriver("jdbc:ductile:file:/opt/hbase/conf/hbase-site.xml");
	assertEquals(DuctileDriver.class, driver.getClass());
    }

    @Test
    public void testGetConnection() throws SQLException {
	Connection connection = DriverManager.getConnection("jdbc:ductile:file:/opt/hbase/conf/hbase-site.xml");
	assertEquals(DuctileConnection.class, connection.getClass());
    }

    @Test
    public void testVersion() throws SQLException {
	Driver driver = DriverManager.getDriver("jdbc:ductile:file:/opt/hbase/conf/hbase-site.xml");
	assumeThat(driver.getClass(), equalTo(DuctileDriver.class));
	String[] versionSplit = BuildInformation.getVersion().split("\\.");
	assertEquals((int) Integer.valueOf(versionSplit[0]), driver.getMajorVersion());
	assertEquals((int) Integer.valueOf(versionSplit[1]), driver.getMinorVersion());
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
	Driver driver = DriverManager.getDriver("jdbc:ductile:file:/opt/hbase/conf/hbase-site.xml");
	assumeThat(driver.getClass(), equalTo(DuctileDriver.class));
	DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo("jdbc:ductile:file:/opt/hbase/conf/hbase-site.xml",
		new Properties());
	assertNotNull(propertyInfo);
	propertyInfo = driver.getPropertyInfo("jdbc:wrong:", new Properties());
	assertNull(propertyInfo);
    }

}
