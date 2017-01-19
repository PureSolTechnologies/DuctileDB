package com.puresoltechnologies.ductiledb.bigtable;

/**
 * This class contains functionality and factories for {@link BigTable} tests
 * and tests which are based on it.
 * 
 * @author Rick-Rainer Ludwig
 */
public class BigTableTestUtils {

    public static BigTableConfiguration createConfiguration() {
	BigTableConfiguration configuration = new BigTableConfiguration();
	return configuration;
    }

}
