package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class DatabaseEngineConfigurationTest {

    @Test
    public void test() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = getClass().getResourceAsStream("/database-engine.yml")) {
	    DatabaseEngineConfiguration configuration = yaml.loadAs(inputStream, DatabaseEngineConfiguration.class);
	    assertNotNull(configuration);
	}
    }

}
