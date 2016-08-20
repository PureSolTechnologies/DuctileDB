package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class DuctileDBConfigurationTest {

    @Test
    public void test() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = AbstractDuctileDBTest.DEFAULT_TEST_CONFIG_URL.openStream()) {
	    yaml.loadAs(inputStream, DuctileDBConfiguration.class);
	}
    }

}
