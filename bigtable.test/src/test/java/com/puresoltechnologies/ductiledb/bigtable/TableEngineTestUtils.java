package com.puresoltechnologies.ductiledb.bigtable;

/**
 * This class contains functionality and factories for {@link TableEngine} tests
 * and tests which are based on it.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TableEngineTestUtils {

    public static BigTableEngineConfiguration createConfiguration() {
	BigTableEngineConfiguration configuration = new BigTableEngineConfiguration();
	return configuration;
    }

}
