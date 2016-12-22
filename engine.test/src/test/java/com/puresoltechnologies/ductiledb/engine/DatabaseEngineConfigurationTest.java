package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.engine.DatabaseEngineConfiguration;

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
