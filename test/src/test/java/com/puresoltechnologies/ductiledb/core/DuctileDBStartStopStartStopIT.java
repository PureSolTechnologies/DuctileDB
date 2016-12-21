package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test assures an empty storage directory and starts, stops, starts and
 * stops again DuctileDB to assure consistency in directories.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBStartStopStartStopIT {

    private static DuctileDBConfiguration configuration = null;

    /**
     * Initializes the database.
     * 
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws ServiceException
     * @throws IOException
     * @throws SchemaException
     */
    @BeforeClass
    public static void initializeDuctileDB() throws IOException {
	configuration = AbstractDuctileDBTest.readTestConfigration();
	assertNotNull("No configuration loaded.", configuration);

    }

    @Test
    public void test() throws IOException, SQLException {
	System.out.println("===== 1st start of database =====");
	DuctileDBBootstrap.start(configuration);
	DuctileDB ductileDB1 = DuctileDBBootstrap.getInstance();
	assertNotNull("DuctileDB is null.", ductileDB1);
	DuctileDBBootstrap.stop();

	System.out.println("===== 2nd start of database =====");
	DuctileDBBootstrap.start(configuration);
	DuctileDB ductileDB2 = DuctileDBBootstrap.getInstance();
	assertNotNull("DuctileDB is null.", ductileDB2);
	DuctileDBBootstrap.stop();
    }

}
