package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.bigtable.BigTableEngineConfiguration;

public class DatabaseEngineConfigurationTest {

    @Test
    public void test() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = getClass().getResourceAsStream("/database-engine.yml")) {
	    BigTableEngineConfiguration configuration = yaml.loadAs(inputStream, BigTableEngineConfiguration.class);
	    assertNotNull(configuration);
	}
    }

}
