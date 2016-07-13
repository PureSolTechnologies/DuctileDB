package com.puresoltechnologies.ductiledb.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDB;

public class AbstractDuctileDBTest {

    public static File hadoopHome = new File("/opt/hadoop");
    public static File hbaseHome = new File("/opt/hbase");
    private static DuctileDB ductileDB = null;

    /**
     * Initializes the database.
     * 
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws ServiceException
     * @throws IOException
     */
    @BeforeClass
    public static void initializeDuctileDB()
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	assertThat("HBase home must exist.", hbaseHome.exists());
	assertThat("Hadoop home must exist.", hadoopHome.exists());
	ductileDB = createDuctileDB();
	assertNotNull("DuctileDB is null.", ductileDB);
    }

    public static DuctileDB createDuctileDB() throws FileNotFoundException, IOException, ServiceException {
	return DuctileDBFactory.connect(hadoopHome, hbaseHome);
    }

    /**
     * Shuts down DuctileDB after test.
     * 
     * @throws IOException
     *             is thrown in case of I/O issues.
     */
    @AfterClass
    public static void shutdownDuctileDB() throws IOException {
	try {
	    ductileDB.close();
	} finally {
	    ductileDB = null;
	}
    }

    /**
     * Returns the initialized and connected database.
     * 
     * @return A {@link DuctileDB} object is returned, ready for use.
     */
    protected static DuctileDB getDuctileDB() {
	return ductileDB;
    }
}
